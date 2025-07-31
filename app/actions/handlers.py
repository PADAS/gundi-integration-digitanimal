import httpx
import logging

import app.actions.client as client

from datetime import datetime, timedelta, timezone
from app.actions.configurations import (
    AuthenticateConfig,
    PullObservationsConfig, PullHistoricalObservationsConfig,
    get_auth_config
)
from app.services.activity_logger import activity_logger
from app.services.gundi import send_observations_to_gundi
from app.services.state import IntegrationStateManager
from app.services.utils import generate_batches

logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()


DIGITANIMAL_BASE_URL = "https://digitanimalapp.com/api/"


def transform(device):
    device_id = device.DEVICE_COLLAR
    recorded_at = device.DEVICE_TIME

    lat = device.LAT
    lon = device.LNG

    device_info = device.dict()
    for key in ["DEVICE_COLLAR", "DEVICE_TIME", "LAT", "LNG"]:
        device_info.pop(key, None)

    return {
        "source_name": device_id,
        "source": device_id,
        "type": "tracking-device",
        "subject_type": "vehicle",
        "recorded_at": recorded_at,
        "location": {
            "lat": lat,
            "lon": lon
        },
        "additional": device_info
    }


async def action_auth(integration, action_config: AuthenticateConfig):
    logger.info(f"Executing 'auth' action with integration ID {integration.id} and action_config {action_config}...")

    base_url = integration.base_url or DIGITANIMAL_BASE_URL
    auth = {
        "username": action_config.username,
        "password": action_config.password.get_secret_value(),
    }

    try:
        devices_response = await client.get_devices_observations(integration.id, base_url, auth)
        # DigitAnimal does not return error codes when the auth fails, the lack of 'devices' is the only indication of failure
        if not devices_response.data.devices:
            logger.error(f"Failed to authenticate with integration {integration.id} using {action_config}")
            return {"valid_credentials": False, "message": "Bad credentials"}
        return {"valid_credentials": True}
    except httpx.HTTPStatusError as e:
        return {"error": True, "status_code": e.response.status_code}


@activity_logger()
async def action_pull_observations(integration, action_config: PullObservationsConfig):
    logger.info(f"Executing 'pull_observations' action with integration ID {integration.id} and action_config {action_config}...")

    base_url = integration.base_url or DIGITANIMAL_BASE_URL
    auth_config = get_auth_config(integration)
    auth = {
        "username": auth_config.username,
        "password": auth_config.password.get_secret_value(),
    }

    try:
        devices_response = await client.get_devices_observations(integration.id, base_url, auth)
        # Check if there are devices associated with the account (auth was successful and the account is active)
        devices = devices_response.data.devices
        if devices:
            observations = []
            observations_extracted = 0
            logger.info(f"Found {len(devices)} devices for integration {integration.id} Account: {auth_config.username}")
            for device in devices:
                # fix device.DEVICE_TIME timezone
                recorded_at = device.DEVICE_TIME
                time_delta = timedelta(hours=action_config.gmt_offset)
                timezone_object = timezone(time_delta)

                device.DEVICE_TIME = recorded_at.replace(tzinfo=timezone_object)

                if device_state := await state_manager.get_state(
                    integration_id=integration.id,
                    action_id="pull_observations",
                    source_id=device.DEVICE_COLLAR
                ):
                    # Check if the device has new observations since the last pull
                    latest_device_datetime = datetime.fromisoformat(device_state["latest_device_datetime"])
                    if device.DEVICE_TIME > latest_device_datetime:
                        observations.append(transform(device))
                    else:
                        logger.info(f"Filtering observation {device.DEVICE_TIME} for device {device.DEVICE_COLLAR}")
                else:
                    # new observation
                    observations.append(transform(device))


            if observations:
                logger.info(f"Sending {len(observations)} observations to Gundi")
                for i, batch in enumerate(generate_batches(observations, 200)):
                    logger.info(f'Sending observations batch #{i}: {len(batch)} observations. Username: {auth_config.username}')
                    response = await send_observations_to_gundi(observations=batch, integration_id=integration.id)
                    observations_extracted += len(response)

                # Save latest device updated_at
                for obs in observations:
                    await state_manager.set_state(
                        integration_id=integration.id,
                        action_id="pull_observations",
                        source_id=obs["source"],
                        state={
                            "latest_device_datetime": obs["recorded_at"].isoformat()
                        }
                    )

            return {"observations_extracted": observations_extracted}
        else:
            logger.warning(f"No devices found for integration {integration.id} Account: {auth_config.username}")
            return {"devices_triggered": 0}
    except Exception as e:
        message = f"Error while pulling observations for integration {integration.id} using {auth_config}. Exception: {e}"
        logger.exception(message)
        raise

@activity_logger()
async def action_pull_historical_observations(integration, action_config: PullHistoricalObservationsConfig):
    logger.info(f"Executing 'pull_historical_observations' action with integration ID {integration.id} and action_config {action_config}...")

    base_url = integration.base_url or DIGITANIMAL_BASE_URL
    auth_config = get_auth_config(integration)
    auth = {
        "username": auth_config.username,
        "password": auth_config.password.get_secret_value(),
    }

    params = {
        "init_date": action_config.start_date,
        "end_date": action_config.end_date
    }

    try:
        devices_response = await client.get_devices_observations(
            integration.id,
            base_url,
            auth,
            params
        )
        # Check if there are devices associated with the account (auth was successful and the account is active)
        devices_historical = devices_response.data.history
        if devices_historical:
            observations = []
            observations_extracted = 0
            logger.info(f"Found {len(devices_historical)} devices for integration {integration.id} Account: {auth_config.username}")

            observations.extend(transform(device) for device in devices_historical)

            if observations:
                logger.info(f"Sending {len(observations)} observations to Gundi")
                for i, batch in enumerate(generate_batches(observations, 200)):
                    logger.info(f'Sending observations batch #{i}: {len(batch)} observations. Username: {auth_config.username}')
                    response = await send_observations_to_gundi(observations=batch, integration_id=integration.id)
                    observations_extracted += len(response)
            return {"observations_extracted": observations_extracted}
        else:
            logger.warning(f"No devices found for integration {integration.id} Account: {auth_config.username}")
            return {"devices_triggered": 0}
    except Exception as e:
        message = f"Error while pulling observations for integration {integration.id} using {auth_config}. Exception: {e}"
        logger.exception(message)
        raise
