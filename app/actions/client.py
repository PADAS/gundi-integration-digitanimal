import logging

from datetime import datetime

import pydantic
import httpx
from pydantic import root_validator
from typing import List, Optional

from app.services.state import IntegrationStateManager
from app.actions.configurations import AuthenticateConfig


state_manager = IntegrationStateManager()
logger = logging.getLogger(__name__)


# Exception classes
class DigitAnimalUnauthorizedException(Exception):
    def __init__(self, message: str, error: Exception = None, status_code=401):
        self.status_code = status_code
        self.message = message
        self.error = error
        super().__init__(f"'{self.status_code}: {self.message}, Error: {self.error}'")


class DigitAnimalErrorException(Exception):
    def __init__(self, message: str, error: Exception = None, status_code=500):
        self.status_code = status_code
        self.message = message
        self.error = error
        super().__init__(f"'{self.status_code}: {self.message}, Error: {self.error}'")


# Pydantic models (representing integration objects to receive/manipulate info from the external API)
class DigitAnimalHistoricalRequestParams(pydantic.BaseModel):
    init_date: str
    end_date: str

    @root_validator(pre=True)
    def parse_datetime(cls, values):
        for key, val in values.items():
            values[key] = val.strftime("%Y-%m-%d %H:%M:%S")
        return values


class DigitAnimalDataResponse(pydantic.BaseModel):
    DEVICE_COLLAR: str
    LAT: float
    LNG: float
    DEVICE_TIME: datetime
    DEVICE_ALARM: Optional[bool]
    DEVICE_LOCATION: Optional[bool]
    DEVICE_TEMPERATURE: Optional[bool]
    DEVICE_DISTANCE: Optional[bool]
    DEVICE_ACTIVITY: Optional[bool]
    DEVICE_POSITION: Optional[bool]
    RAW_TEMPERATURE: Optional[float]
    RAW_ACC_X: Optional[float]
    RAW_ACC_Y: Optional[float]
    RAW_ACC_Z: Optional[float]


class DigitAnimalData(pydantic.BaseModel):
    devices: Optional[List[DigitAnimalDataResponse]]
    history: List[DigitAnimalDataResponse]


class DigitAnimalResponse(pydantic.BaseModel):
    success: bool
    message: str
    data: DigitAnimalData


async def get_devices_observations(
        integration_id: str,
        base_url: str,
        auth: dict,
        params: dict = None
):
    """
        Call the client's 'get_device_info.php' endpoint (with dates range)

    :param: integration_id: The integration ID
    :param: base_url: The base URL of the DigitAnimal API
    :param: auth: The configuration object containing authentication details
    :param: params: The configuration object containing date range details
    :return: The devices list response
    """

    logger.info(f"Getting devices observations for integration: '{integration_id}' Username: '{auth['username']}'")

    url = f"{base_url}get_device_info.php"

    if params:
        params = DigitAnimalHistoricalRequestParams(**params).dict()

    async with httpx.AsyncClient(timeout=httpx.Timeout(connect=10.0, read=30.0, write=15.0, pool=5.0)) as session:
        response = await session.get(
            url=url,
            params=params,
            auth=(auth['username'], auth['password']),
        )
        response.raise_for_status()

    response_json = response.json()

    logger.info(f"Got devices observations for username: '{auth['username']}'")
    logger.debug(f"Response: {response_json}")

    return DigitAnimalResponse.parse_obj(response_json)
