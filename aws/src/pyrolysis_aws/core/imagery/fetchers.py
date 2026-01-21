from __future__ import annotations

import logging
import math
from typing import Literal

import aiohttp

from ..aws_clients.secrets import get_secret_json

_logger = logging.getLogger(__name__)

MAPBOX_TILESET = "mapbox.satellite"
MAPBOX_FORMAT = "jpg"
GOOGLE_BASE_URL = "https://maps.googleapis.com/maps/api/staticmap"

_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class ImageryFetchError(Exception):
    """Raised when imagery fetch fails after retries."""

    def __init__(
        self,
        source: Literal["mapbox", "google"],
        status_code: int | None,
        message: str,
        attempts: int,
    ) -> None:
        self.source = source
        self.status_code = status_code
        self.attempts = attempts
        super().__init__(f"{source} fetch failed after {attempts} attempts: {message}")


def _mapbox_url(z: int, x: int, y: int, token: str) -> str:
    return (
        f"https://api.mapbox.com/v4/{MAPBOX_TILESET}/{z}/{x}/{y}.{MAPBOX_FORMAT}"
        f"?access_token={token}"
    )


def _google_url(lat: float, lon: float, zoom: int, api_key: str) -> str:
    return (
        f"{GOOGLE_BASE_URL}?center={lat},{lon}&zoom={zoom}"
        f"&size=640x640&scale=2&maptype=satellite&key={api_key}"
    )


def tile_to_lonlat_bounds(x: int, y: int, z: int) -> tuple[float, float, float, float]:
    """Return (min_lon, min_lat, max_lon, max_lat) for a tile in EPSG:4326."""
    n = 2**z

    def tile_x_to_lon(tx: int) -> float:
        return tx / n * 360.0 - 180.0

    def tile_y_to_lat(ty: int) -> float:
        y2 = math.pi * (1 - 2 * ty / n)
        return math.degrees(math.atan(math.sinh(y2)))

    min_lon = tile_x_to_lon(x)
    max_lon = tile_x_to_lon(x + 1)
    min_lat = tile_y_to_lat(y + 1)
    max_lat = tile_y_to_lat(y)
    return (min_lon, min_lat, max_lon, max_lat)


def tile_center_latlon(x: int, y: int, z: int) -> tuple[float, float]:
    """Return (lat, lon) of the tile center."""
    min_lon, min_lat, max_lon, max_lat = tile_to_lonlat_bounds(x, y, z)
    center_lat = (min_lat + max_lat) / 2.0
    center_lon = (min_lon + max_lon) / 2.0
    return (center_lat, center_lon)


async def _fetch_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    *,
    source: Literal["mapbox", "google"],
    max_retries: int,
    timeout: float,
    backoff_base: float = 0.5,
) -> bytes:
    """
    Fetch URL with exponential backoff retry.

    Args:
        session: aiohttp session to use
        url: Target URL
        source: "mapbox" or "google" for error reporting
        max_retries: Maximum number of attempts
        timeout: Per-request timeout in seconds
        backoff_base: Base for exponential backoff

    Returns:
        Response bytes on success

    Raises:
        ImageryFetchError: On failure after all retries
    """
    import asyncio

    last_status: int | None = None
    last_message: str = ""

    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=timeout)
            ) as response:
                if response.status == 200:
                    return await response.read()

                last_status = response.status
                last_message = await response.text()

                if response.status not in _RETRYABLE_STATUS:
                    # Non-retryable error (4xx except 429)
                    raise ImageryFetchError(
                        source, response.status, last_message, attempt
                    )

                # Retryable error - log and continue
                _logger.warning(
                    "%s fetch attempt %d/%d failed with status %d",
                    source,
                    attempt,
                    max_retries,
                    response.status,
                )

        except aiohttp.ClientError as exc:
            last_status = None
            last_message = str(exc)
            _logger.warning(
                "%s fetch attempt %d/%d failed with error: %s",
                source,
                attempt,
                max_retries,
                exc,
            )

        except ImageryFetchError:
            raise

        # Don't sleep after the last attempt
        if attempt < max_retries:
            sleep_for = backoff_base * (2 ** (attempt - 1))
            await asyncio.sleep(sleep_for)

    raise ImageryFetchError(source, last_status, last_message, max_retries)


async def fetch_mapbox_tile(
    z: int,
    x: int,
    y: int,
    *,
    secrets_id: str,
    session: aiohttp.ClientSession | None = None,
    max_retries: int = 3,
    timeout: float = 10.0,
) -> bytes:
    """
    Fetch a Mapbox satellite tile.

    Args:
        z, x, y: Tile coordinates
        secrets_id: Secrets Manager secret ID containing MAPBOX_TOKEN
        session: Optional aiohttp session (created if not provided)
        max_retries: Maximum retry attempts
        timeout: Per-request timeout in seconds

    Returns:
        Image bytes (JPEG)

    Raises:
        ImageryFetchError: On failure after retries
    """
    secrets = get_secret_json(secrets_id)
    token = secrets.get("MAPBOX_TOKEN", "").strip()
    if not token:
        raise RuntimeError("MAPBOX_TOKEN not found in secrets")

    url = _mapbox_url(z, x, y, token)

    close_session = False
    if session is None:
        session = aiohttp.ClientSession()
        close_session = True

    try:
        return await _fetch_with_retry(
            session,
            url,
            source="mapbox",
            max_retries=max_retries,
            timeout=timeout,
        )
    finally:
        if close_session:
            await session.close()


async def fetch_google_tile(
    lat: float,
    lon: float,
    zoom: int,
    *,
    secrets_id: str,
    session: aiohttp.ClientSession | None = None,
    max_retries: int = 3,
    timeout: float = 10.0,
) -> bytes:
    """
    Fetch a Google Maps Static API satellite image.

    Args:
        lat, lon: Coordinates
        zoom: Zoom level
        secrets_id: Secrets Manager secret ID containing GOOGLE_MAPS_API_KEY
        session: Optional aiohttp session (created if not provided)
        max_retries: Maximum retry attempts
        timeout: Per-request timeout in seconds

    Returns:
        Image bytes (PNG)

    Raises:
        ImageryFetchError: On failure after retries
    """
    secrets = get_secret_json(secrets_id)
    api_key = secrets.get("GOOGLE_MAPS_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("GOOGLE_MAPS_API_KEY not found in secrets")

    url = _google_url(lat, lon, zoom, api_key)

    close_session = False
    if session is None:
        session = aiohttp.ClientSession()
        close_session = True

    try:
        return await _fetch_with_retry(
            session,
            url,
            source="google",
            max_retries=max_retries,
            timeout=timeout,
        )
    finally:
        if close_session:
            await session.close()


__all__ = [
    "ImageryFetchError",
    "fetch_mapbox_tile",
    "fetch_google_tile",
    "tile_center_latlon",
    "tile_to_lonlat_bounds",
]
