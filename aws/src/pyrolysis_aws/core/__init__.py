"""Shared core modules for AWS Lambdas."""

from .config import BaseConfig, IngestionConfig, WorkerConfig
from .ddb.runs import set_total_tiles
from .ddb.tilejobs import ClaimResult, ClaimResultData
from .errors import ErrorCode, error_code_from_http_status
from .io import DeadlineExceededError, RetryExhaustedError
from .logging import get_logger, log_structured, timed_stage
from .schema import (
    DEFAULT_GOOGLE_ZOOM,
    JobStatus,
    RunItem,
    RunStatus,
    ProcessTileResult,
    SourceRef,
    TileJobItem,
    TileJobMessage,
    tile_id_for_coords,
)

__all__ = [
    # Config
    "BaseConfig",
    "IngestionConfig",
    "WorkerConfig",
    # Schema
    "DEFAULT_GOOGLE_ZOOM",
    "JobStatus",
    "RunStatus",
    "RunItem",
    "ProcessTileResult",
    "TileJobItem",
    "TileJobMessage",
    "SourceRef",
    "tile_id_for_coords",
    # DDB
    "ClaimResult",
    "ClaimResultData",
    "set_total_tiles",
    # Errors
    "ErrorCode",
    "error_code_from_http_status",
    # HTTP
    "DeadlineExceededError",
    "RetryExhaustedError",
    # Logging
    "get_logger",
    "log_structured",
    "timed_stage",
]
