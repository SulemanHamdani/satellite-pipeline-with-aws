from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field, model_validator

DEFAULT_GOOGLE_ZOOM = 18
COORD_TILE_ID_PRECISION = 6


class SourceRef(BaseModel):
    bucket: str
    key: str


class TileJobMessage(BaseModel):
    """
    SQS message body for a tile processing job.

    For Mapbox: z, x, y are required; tile_id is computed if not provided.
    For Google: lat, lon are required; zoom defaults to DEFAULT_GOOGLE_ZOOM.
    """

    model_config = {"frozen": True}

    run_id: str
    imagery_source: Literal["mapbox", "google"]
    source: SourceRef

    # Mapbox tile fields (tile_id is always computed from z/x/y, not stored in message)
    z: int | None = Field(None, ge=0, le=22)
    x: int | None = Field(None, ge=0)
    y: int | None = Field(None, ge=0)
    region: str | None = None

    # Google coords fields
    lat: float | None = Field(None, ge=-90, le=90)
    lon: float | None = Field(None, ge=-180, le=180)
    zoom: int | None = Field(None, ge=0, le=22)

    @model_validator(mode="after")
    def _validate_source_specific_fields(self) -> "TileJobMessage":
        if self.imagery_source == "mapbox":
            if self.z is None or self.x is None or self.y is None:
                raise ValueError("Mapbox messages require z, x, and y")
            return self

        if self.lat is None or self.lon is None:
            raise ValueError("Google messages require lat and lon")
        return self

    def get_tile_id(self) -> str:
        """
        Compute the canonical tile_id for this message.

        Always computes from coordinates (z/x/y or lat/lon/zoom) to ensure
        consistency. The tile_id field is ignored to prevent mismatched keys.
        """
        if self.imagery_source == "mapbox":
            return f"{self.z}/{self.x}/{self.y}"
        return tile_id_for_coords(
            self.lat, self.lon, self.zoom if self.zoom is not None else DEFAULT_GOOGLE_ZOOM
        )

    def get_zoom(self) -> int:
        """Get zoom level, using default for Google if not specified."""
        if self.zoom is not None:
            return self.zoom
        return DEFAULT_GOOGLE_ZOOM


class RunStatus(str, Enum):
    CREATED = "CREATED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class JobStatus(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


def tile_id_for_coords(lat: float, lon: float, zoom: int) -> str:
    lat_str = f"{lat:.{COORD_TILE_ID_PRECISION}f}"
    lon_str = f"{lon:.{COORD_TILE_ID_PRECISION}f}"
    return f"coord:{lat_str},{lon_str},{zoom}"


class RunItem(BaseModel):
    run_id: str
    status: RunStatus
    total_tiles: int = Field(..., ge=0)
    completed_tiles: int = Field(0, ge=0)
    failed_tiles: int = Field(0, ge=0)
    source_bucket: str
    source_key: str
    created_at_epoch: int = Field(..., ge=0)
    finished_at_epoch: int | None = Field(None, ge=0)


class S3Checkpoint(BaseModel):
    """S3 location for a checkpointed tile image."""

    bucket: str
    key: str


class ClaimResult(str, Enum):
    """Outcome of a claim attempt."""

    CLAIMED = "claimed"
    ALREADY_COMPLETED = "already_completed"
    LOCKED_BY_OTHER = "locked_by_other"


class ClaimResultData(BaseModel):
    """Result of attempting to claim a job."""

    result: ClaimResult
    tile_id: str
    attempt: int | None = None
    claimed_at_epoch: int | None = None
    checkpoint: S3Checkpoint | None = None


class ProcessTileResult(BaseModel):
    """Result of processing a single tile."""

    status: Literal["completed", "skipped"]
    tile_id: str | None = None
    status_ai: str | None = None
    s3_key: str | None = None
    reason: str | None = None


class TileJobItem(BaseModel):
    """
    DynamoDB item representing a tile job.

    This model represents data read from DynamoDB, where tile_id and zoom
    are already stored. Unlike TileJobMessage, these fields should be present.
    """

    model_config = {"frozen": True}

    run_id: str
    tile_id: str
    status: JobStatus
    attempts: int = Field(0, ge=0)
    lock_until_epoch: int | None = Field(None, ge=0)
    started_at_epoch: int | None = Field(None, ge=0)
    last_claimed_at_epoch: int | None = Field(None, ge=0)
    finished_at_epoch: int | None = Field(None, ge=0)
    error_code: str | None = None
    error_message: str | None = None

    imagery_source: Literal["mapbox", "google"]
    z: int | None = Field(None, ge=0, le=22)
    x: int | None = Field(None, ge=0)
    y: int | None = Field(None, ge=0)
    region: str | None = None
    lat: float | None = Field(None, ge=-90, le=90)
    lon: float | None = Field(None, ge=-180, le=180)
    zoom: int | None = Field(None, ge=0, le=22)

    s3_bucket: str | None = None
    s3_key: str | None = None
    s3_url: str | None = None
    status_ai: str | None = None
    reasoning: str | None = None
    openai_usage: dict | None = None

    @model_validator(mode="after")
    def _validate_metadata(self) -> "TileJobItem":
        if self.imagery_source == "mapbox":
            if self.z is None or self.x is None or self.y is None:
                raise ValueError("Mapbox jobs require z, x, and y")
            return self

        if self.lat is None or self.lon is None:
            raise ValueError("Google jobs require lat and lon")
        return self

    def get_zoom(self) -> int:
        """Get zoom level, using default for Google if not specified."""
        if self.zoom is not None:
            return self.zoom
        return DEFAULT_GOOGLE_ZOOM


__all__ = [
    "SourceRef",
    "TileJobMessage",
    "RunStatus",
    "JobStatus",
    "RunItem",
    "TileJobItem",
    "S3Checkpoint",
    "ClaimResult",
    "ClaimResultData",
    "ProcessTileResult",
    "DEFAULT_GOOGLE_ZOOM",
    "tile_id_for_coords",
]
