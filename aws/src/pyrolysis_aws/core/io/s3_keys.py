from __future__ import annotations

from ..schema import COORD_TILE_ID_PRECISION, DEFAULT_GOOGLE_ZOOM


def mapbox_tile_key(run_id: str, z: int, x: int, y: int, ext: str = "png") -> str:
    return f"runs/{run_id}/tiles/z={z}/x={x}/y={y}.{ext}"


def google_coord_key(
    run_id: str,
    lat: float,
    lon: float,
    zoom: int | None = None,
    ext: str = "png",
) -> str:
    z = zoom if zoom is not None else DEFAULT_GOOGLE_ZOOM
    lat_str = f"{lat:.{COORD_TILE_ID_PRECISION}f}"
    lon_str = f"{lon:.{COORD_TILE_ID_PRECISION}f}"
    return f"runs/{run_id}/coords/lat={lat_str}/lon={lon_str}/z={z}.{ext}"


__all__ = ["mapbox_tile_key", "google_coord_key"]
