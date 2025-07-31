import pytest
from unittest.mock import AsyncMock, patch, MagicMock

import app.actions.client as client

@pytest.mark.asyncio
async def test_get_devices_observations_success():
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=MagicMock(
            json=MagicMock(return_value={
                "success": True,
                "message": "ok",
                "data": {
                    "devices": [
                        {
                            "id": 1,
                            "name": "Device1",
                            "DEVICE_COLLAR": "collar1",
                            "LAT": 10.0,
                            "LNG": 20.0,
                            "DEVICE_TIME": "2024-01-01T00:00:00Z"
                        },
                        {
                            "id": 2,
                            "name": "Device2",
                            "DEVICE_COLLAR": "collar2",
                            "LAT": 11.0,
                            "LNG": 21.0,
                            "DEVICE_TIME": "2024-01-01T01:00:00Z"
                        },
                        {
                            "id": 3,
                            "name": "Device3",
                            "DEVICE_COLLAR": "collar3",
                            "LAT": 12.0,
                            "LNG": 22.0,
                            "DEVICE_TIME": "2024-01-01T02:00:00Z"
                        }
                    ],
                    "history": []
                }
            }),
            status_code=200
    ))) as mock_get:
        with patch("httpx.AsyncClient.__aenter__", new=AsyncMock(return_value=MagicMock(get=mock_get))):
            result = await client.get_devices_observations("id", "url", {"username": "u", "password": "p"})
            assert hasattr(result, "data")

@pytest.mark.asyncio
async def test_get_devices_observations_http_error():
    # Simulate HTTP error
    with patch("httpx.AsyncClient.get", new=AsyncMock(side_effect=Exception("HTTP error"))):
        with pytest.raises(Exception):
            await client.get_devices_observations("id", "url", {"username": "u", "password": "p"})

@pytest.mark.asyncio
async def test_get_devices_observations_invalid_response():
    # Simulate invalid response (missing data)
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=MagicMock(json=MagicMock(return_value={}), status_code=200))):
        with patch("httpx.AsyncClient.__aenter__", new=AsyncMock(return_value=MagicMock(get=AsyncMock()))):
            with pytest.raises(Exception):
                await client.get_devices_observations("id", "url", {"username": "u", "password": "p"})
