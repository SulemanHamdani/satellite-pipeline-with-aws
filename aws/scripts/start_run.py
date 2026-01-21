from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import sys
import time
from pathlib import Path
from typing import Iterable, Iterator, Literal, Sequence

_AWS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_AWS_ROOT / "src"))

from pyrolysis_aws.core.aws_clients import s3, sqs
from pyrolysis_aws.core.config import IngestionConfig
from pyrolysis_aws.core.ddb.runs import create_run, set_total_tiles
from pyrolysis_aws.core.schema import TileJobMessage

ImagerySource = Literal["mapbox", "google"]


def _detect_source_from_header(header: Sequence[str]) -> ImagerySource | None:
    lowered = {value.strip().lower() for value in header if value is not None}
    if {"lat", "lon"}.issubset(lowered):
        return "google"
    if {"z", "x", "y"}.issubset(lowered):
        return "mapbox"
    return None


def _iter_csv_rows(
    body: io.TextIOBase,
    *,
    source_hint: ImagerySource | None,
) -> tuple[ImagerySource, Iterator[dict[str, str] | list[str]]]:
    reader = csv.reader(body)
    first_row = next(reader, None)
    if first_row is None:
        raise ValueError("CSV is empty")

    detected = _detect_source_from_header(first_row)
    if detected:
        fieldnames = [value.strip().lower() for value in first_row]
        dict_reader = csv.DictReader(body, fieldnames=fieldnames)
        return detected, iter(dict_reader)

    if source_hint is None:
        raise ValueError(
            "Unable to detect imagery source from header. Provide --source mapbox|google."
        )

    def row_iter() -> Iterator[list[str]]:
        yield first_row
        for row in reader:
            yield row

    return source_hint, row_iter()


def _parse_mapbox_row(row: dict[str, str] | list[str]) -> tuple[int, int, int, str | None]:
    if isinstance(row, dict):
        z = int(row["z"])
        x = int(row["x"])
        y = int(row["y"])
        region = row.get("region") or None
        return z, x, y, region
    if len(row) < 3:
        raise ValueError("Mapbox CSV rows must have z,x,y (and optional region)")
    z = int(row[0])
    x = int(row[1])
    y = int(row[2])
    region = row[3] if len(row) > 3 and row[3] else None
    return z, x, y, region


def _parse_google_row(row: dict[str, str] | list[str]) -> tuple[float, float, int | None]:
    if isinstance(row, dict):
        lat = float(row["lat"])
        lon = float(row["lon"])
        zoom_raw = row.get("zoom") or None
        zoom = int(zoom_raw) if zoom_raw not in (None, "") else None
        return lat, lon, zoom
    if len(row) < 2:
        raise ValueError("Google CSV rows must have lat,lon (and optional zoom)")
    lat = float(row[0])
    lon = float(row[1])
    zoom = int(row[2]) if len(row) > 2 and row[2] else None
    return lat, lon, zoom


def _iter_messages(
    rows: Iterable[dict[str, str] | list[str]],
    *,
    run_id: str,
    source: ImagerySource,
    source_bucket: str,
    source_key: str,
) -> Iterator[TileJobMessage]:
    for row in rows:
        if isinstance(row, dict):
            if all(value in (None, "", " ") for value in row.values()):
                continue
        else:
            if all(value in (None, "", " ") for value in row):
                continue

        if source == "mapbox":
            z, x, y, region = _parse_mapbox_row(row)
            payload = {
                "run_id": run_id,
                "imagery_source": "mapbox",
                "z": z,
                "x": x,
                "y": y,
                "region": region,
                "source": {"bucket": source_bucket, "key": source_key},
            }
        else:
            lat, lon, zoom = _parse_google_row(row)
            payload = {
                "run_id": run_id,
                "imagery_source": "google",
                "lat": lat,
                "lon": lon,
                "zoom": zoom,
                "source": {"bucket": source_bucket, "key": source_key},
            }

        yield TileJobMessage.model_validate(payload)


def _send_batch(queue_url: str, entries: list[dict[str, str]]) -> None:
    if not entries:
        return
    response = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
    failures = response.get("Failed", [])
    if failures:
        raise RuntimeError(f"SQS batch send failed: {failures}")


def _compute_run_id(bucket: str, key: str, etag: str) -> str:
    raw = f"{bucket}:{key}:{etag}".encode("utf-8")
    digest = hashlib.sha1(raw).hexdigest()[:12]
    return f"run_{digest}"


def _get_s3_body(bucket: str, key: str) -> tuple[io.TextIOBase, str]:
    head = s3.head_object(Bucket=bucket, Key=key)
    etag = head.get("ETag", "").strip('"')
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    text_stream = io.TextIOWrapper(body, encoding="utf-8-sig")
    return text_stream, etag


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest CSV from S3 into SQS tile jobs.")
    parser.add_argument("--bucket", required=True, help="S3 bucket containing the CSV")
    parser.add_argument("--key", required=True, help="S3 key for the CSV")
    parser.add_argument("--run-id", default=None, help="Override run_id (optional)")
    parser.add_argument(
        "--source",
        choices=["mapbox", "google"],
        default=None,
        help="Imagery source if CSV header is missing",
    )
    parser.add_argument("--dry-run", action="store_true", help="Validate only, no SQS sends")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    config = IngestionConfig.from_env()

    text_stream, etag = _get_s3_body(args.bucket, args.key)
    run_id = args.run_id or _compute_run_id(args.bucket, args.key, etag)

    create_run(
        config.runs_table,
        run_id,
        args.bucket,
        args.key,
        total_tiles=0,
    )

    source, rows = _iter_csv_rows(text_stream, source_hint=args.source)

    total = 0
    batch: list[dict[str, str]] = []
    start = time.time()

    for message in _iter_messages(
        rows,
        run_id=run_id,
        source=source,
        source_bucket=args.bucket,
        source_key=args.key,
    ):
        total += 1
        body = json.dumps(message.model_dump(exclude_none=True))
        batch.append({"Id": str(total), "MessageBody": body})

        if len(batch) == 10:
            if not args.dry_run:
                _send_batch(config.tile_jobs_queue_url, batch)
            batch = []

    if batch and not args.dry_run:
        _send_batch(config.tile_jobs_queue_url, batch)

    set_total_tiles(config.runs_table, run_id, total_tiles=total)

    elapsed = time.time() - start
    print(f"run_id={run_id} source={source} total={total} elapsed={elapsed:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
