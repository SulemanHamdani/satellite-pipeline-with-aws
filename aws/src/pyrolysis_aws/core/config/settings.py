from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import TypeVar

_logger_configured = False


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _configure_logging(level: str) -> None:
    global _logger_configured
    if not _logger_configured:
        logging.basicConfig(
            level=level,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )
        _logger_configured = True


T = TypeVar("T", bound="BaseConfig")


@dataclass(frozen=True, slots=True)
class BaseConfig:
    aws_region: str
    s3_bucket: str
    runs_table: str
    tilejobs_table: str
    secrets_id: str
    job_stale_lock_seconds: int
    max_retries: int
    request_timeout: float
    log_level: str

    @classmethod
    def _base_kwargs(cls) -> dict:
        """Load base config values from environment."""
        aws_region = os.getenv("AWS_REGION", "us-east-1")
        s3_bucket = _require("S3_BUCKET")
        runs_table = _require("DDB_RUNS_TABLE")
        tilejobs_table = _require("DDB_TILEJOBS_TABLE")
        secrets_id = _require("PIPELINE_SECRETS_ID")
        job_stale_lock_seconds = int(os.getenv("JOB_STALE_LOCK_SECONDS", "900"))
        max_retries = int(os.getenv("PIPELINE_MAX_RETRIES", "3"))
        request_timeout = float(os.getenv("PIPELINE_REQUEST_TIMEOUT", "10"))
        log_level = os.getenv("LOG_LEVEL", "INFO")

        _configure_logging(log_level)

        return {
            "aws_region": aws_region,
            "s3_bucket": s3_bucket,
            "runs_table": runs_table,
            "tilejobs_table": tilejobs_table,
            "secrets_id": secrets_id,
            "job_stale_lock_seconds": job_stale_lock_seconds,
            "max_retries": max_retries,
            "request_timeout": request_timeout,
            "log_level": log_level,
        }

    @classmethod
    def from_env(cls: type[T]) -> T:
        return cls(**cls._base_kwargs())


@dataclass(frozen=True, slots=True)
class IngestionConfig(BaseConfig):
    tile_jobs_queue_url: str = field(default="")

    @classmethod
    def from_env(cls) -> "IngestionConfig":
        kwargs = cls._base_kwargs()
        kwargs["tile_jobs_queue_url"] = _require("TILE_JOBS_QUEUE_URL")
        return cls(**kwargs)


@dataclass(frozen=True, slots=True)
class WorkerConfig(BaseConfig):
    @classmethod
    def from_env(cls) -> "WorkerConfig":
        return cls(**cls._base_kwargs())


__all__ = ["BaseConfig", "IngestionConfig", "WorkerConfig"]
