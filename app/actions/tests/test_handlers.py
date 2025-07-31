import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch, MagicMock

import app.actions.handlers as handlers
from app import settings

@pytest_asyncio.fixture
def auth_config():
    class AuthConfig:
        username = "user"
        password = MagicMock()
        password.get_secret_value.return_value = "pass"
    return AuthConfig()

@pytest.mark.asyncio
async def test_action_auth_success(integration_v2, auth_config):
    with patch("app.actions.client.get_devices_observations", new=AsyncMock()) as mock_get:
        mock_get.return_value = MagicMock(data=MagicMock(devices=[1,2,3]))
        result = await handlers.action_auth(integration_v2, auth_config)
        assert result["valid_credentials"] is True

@pytest.mark.asyncio
async def test_action_auth_failure(integration_v2, auth_config):
    with patch("app.actions.client.get_devices_observations", new=AsyncMock()) as mock_get:
        mock_get.return_value = MagicMock(data=MagicMock(devices=None))
        result = await handlers.action_auth(integration_v2, auth_config)
        assert result["valid_credentials"] is False

@pytest.mark.asyncio
async def test_action_auth_http_error(integration_v2, auth_config):
    with patch("app.actions.client.get_devices_observations", new=AsyncMock(side_effect=Exception("fail"))):
        with pytest.raises(Exception):
            await handlers.action_auth(integration_v2, auth_config)

@pytest.mark.asyncio
async def test_action_pull_observations_success(mocker, mock_publish_event, integration_v2, auth_config):
    integration = integration_v2
    # Modify auth config
    integration.configurations[2].data = {"username": "user", "password": "pass"}

    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=None)
    mocker.patch("app.services.state.IntegrationStateManager.set_state", return_value=None)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.trigger_action", return_value=None)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.execute_action", return_value=None)

    device = MagicMock()
    device.DEVICE_COLLAR = "collar"
    device.DEVICE_TIME = handlers.datetime.now()
    device.LAT = 1.0
    device.LNG = 2.0
    device.dict.return_value = {}
    devices_response = MagicMock(data=MagicMock(devices=[device]))

    mocker.patch("app.actions.client.get_devices_observations", new=AsyncMock(return_value=devices_response))
    mocker.patch("app.actions.handlers.send_observations_to_gundi", new=AsyncMock(return_value=[1]))

    result = await handlers.action_pull_observations(integration, MagicMock(gmt_offset=0))
    assert result["observations_extracted"] == 1

@pytest.mark.asyncio
async def test_action_pull_observations_no_devices(integration_v2, auth_config):
    integration = integration_v2
    # Modify auth config
    integration.configurations[2].data = {"username": "user", "password": "pass"}
    devices_response = MagicMock(data=MagicMock(devices=None))
    with patch("app.actions.client.get_devices_observations", new=AsyncMock(return_value=devices_response)):
        result = await handlers.action_pull_observations(integration, MagicMock(gmt_offset=0))
        assert result["devices_triggered"] == 0

@pytest.mark.asyncio
async def test_action_pull_observations_exception(integration_v2, auth_config):
    integration = integration_v2
    # Modify auth config
    integration.configurations[2].data = {"username": "user", "password": "pass"}
    with patch("app.actions.client.get_devices_observations", new=AsyncMock(side_effect=Exception("fail"))):
        with pytest.raises(Exception):
            await handlers.action_pull_observations(integration, MagicMock(gmt_offset=0))

@pytest.mark.asyncio
async def test_action_pull_historical_observations_success(mocker, mock_publish_event, integration_v2, auth_config):
    integration = integration_v2
    # Modify auth config
    integration.configurations[2].data = {"username": "user", "password": "pass"}

    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=None)
    mocker.patch("app.services.state.IntegrationStateManager.set_state", return_value=None)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.trigger_action", return_value=None)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.execute_action", return_value=None)

    device = MagicMock()
    device.DEVICE_COLLAR = "collar"
    device.DEVICE_TIME = handlers.datetime.now()
    device.LAT = 1.0
    device.LNG = 2.0
    device.dict.return_value = {}
    devices_response = MagicMock(data=MagicMock(history=[device]))

    mocker.patch("app.actions.client.get_devices_observations", new=AsyncMock(return_value=devices_response))
    mocker.patch("app.actions.handlers.send_observations_to_gundi", new=AsyncMock(return_value=[1]))

    result = await handlers.action_pull_historical_observations(
        integration, MagicMock(start_date="2020-01-01", end_date="2020-01-02", gmt_offset=0)
    )
    assert result["observations_extracted"] == 1

@pytest.mark.asyncio
async def test_action_pull_historical_observations_no_devices(integration_v2, auth_config):
    integration = integration_v2
    # Modify auth config
    integration.configurations[2].data = {"username": "user", "password": "pass"}
    devices_response = MagicMock(data=MagicMock(history=None))
    with patch("app.actions.client.get_devices_observations", new=AsyncMock(return_value=devices_response)):
        result = await handlers.action_pull_historical_observations(
            integration, MagicMock(start_date="2020-01-01", end_date="2020-01-02", gmt_offset=0)
        )
        assert result["devices_triggered"] == 0

@pytest.mark.asyncio
async def test_action_pull_historical_observations_exception(integration_v2, auth_config):
    integration = integration_v2
    # Modify auth config
    integration.configurations[2].data = {"username": "user", "password": "pass"}
    with patch("app.actions.client.get_devices_observations", new=AsyncMock(side_effect=Exception("fail"))):
        with pytest.raises(Exception):
            await handlers.action_pull_historical_observations(
                integration, MagicMock(start_date="2020-01-01", end_date="2020-01-02", gmt_offset=0)
            )
