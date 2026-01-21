from __future__ import annotations

import logging
from typing import Literal

import aioboto3

from .s3_keys import google_coord_key, mapbox_tile_key

_logger = logging.getLogger(__name__)

# Module-level session for connection reuse across warm Lambda invocations
_session: aioboto3.Session | None = None


def _get_session() -> aioboto3.Session:
    """Get or create the aioboto3 session (singleton per Lambda environment)."""
    global _session
    if _session is None:
        _session = aioboto3.Session()
    return _session


async def upload_tile_image(
    image_bytes: bytes,
    *,
    bucket: str,
    run_id: str,
    imagery_source: Literal["mapbox", "google"],
    # Mapbox fields
    z: int | None = None,
    x: int | None = None,
    y: int | None = None,
    # Google fields
    lat: float | None = None,
    lon: float | None = None,
    zoom: int | None = None,
    # Options
    content_type: str | None = None,
) -> tuple[str, str]:
    """
    Upload tile imagery to S3 with deterministic key.

    Args:
        image_bytes: Image data to upload
        bucket: S3 bucket name
        run_id: Run identifier for path prefix
        imagery_source: "mapbox" or "google"
        z, x, y: Mapbox tile coordinates (required if imagery_source="mapbox")
        lat, lon, zoom: Google coordinates (required if imagery_source="google")
        content_type: Optional content type (auto-detected if not provided)

    Returns:
        Tuple of (bucket, key) for the uploaded object

    Raises:
        ValueError: If required coordinates are missing
        botocore.exceptions.ClientError: On S3 upload failure
    """
    # Determine S3 key and content type based on source
    if imagery_source == "mapbox":
        if z is None or x is None or y is None:
            raise ValueError("Mapbox uploads require z, x, y coordinates")
        key = mapbox_tile_key(run_id, z, x, y, ext="jpg")
        content_type = content_type or "image/jpeg"
    else:
        if lat is None or lon is None:
            raise ValueError("Google uploads require lat, lon coordinates")
        key = google_coord_key(run_id, lat, lon, zoom, ext="png")
        content_type = content_type or "image/png"

    _logger.info("Uploading to s3://%s/%s", bucket, key)

    session = _get_session()
    async with session.client("s3") as client:
        await client.put_object(
            Bucket=bucket,
            Key=key,
            Body=image_bytes,
            ContentType=content_type,
        )

    return bucket, key


async def download_image(bucket: str, key: str) -> bytes:
    """
    Download image from S3.

    Used on retry when S3 checkpoint exists — avoids re-fetching from imagery API.
    """
    _logger.info("Downloading from s3://%s/%s", bucket, key)

    session = _get_session()
    async with session.client("s3") as client:
        response = await client.get_object(Bucket=bucket, Key=key)
        return await response["Body"].read()


def s3_url(bucket: str, key: str, region: str = "us-east-1") -> str:
    """Build an S3 URL from bucket and key."""
    return f"https://{bucket}.s3.{region}.amazonaws.com/{key}"


__all__ = ["upload_tile_image", "download_image", "s3_url"]
