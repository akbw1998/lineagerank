"""Download Divvy Chicago bike-share trip data (Jan 2024).

Source:
    Lyft Divvy Bikes — System Data (publicly available under Divvy Data License Agreement)
    https://divvybikes.com/system-data
    Direct ZIP: https://divvy-tripdata.s3.amazonaws.com/202401-divvy-tripdata.zip

Dataset citation:
    Lyft Bikes and Scooters, LLC, "Divvy Trip Data," Jan 2024.
    Available: https://divvy-tripdata.s3.amazonaws.com/202401-divvy-tripdata.zip
    License: https://divvybikes.com/data-license-agreement

Schema (202401-divvy-tripdata.csv):
    ride_id, rideable_type, started_at, ended_at,
    start_station_name, start_station_id, end_station_name, end_station_id,
    start_lat, start_lng, end_lat, end_lng, member_casual

Row count (Jan 2024): 144,873 trips
"""
from __future__ import annotations

import io
import shutil
import urllib.request
import zipfile
from pathlib import Path

SOURCE_URL = "https://divvy-tripdata.s3.amazonaws.com/202401-divvy-tripdata.zip"
SOURCE_CITATION = (
    'Lyft Bikes and Scooters, LLC, "Divvy Trip Data," Jan 2024. '
    "Available: https://divvy-tripdata.s3.amazonaws.com/202401-divvy-tripdata.zip"
)

ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "data" / "raw" / "divvy"
OUT_CSV = OUT_DIR / "202401-divvy-tripdata.csv"


def download() -> None:
    if OUT_CSV.exists():
        print(f"Already present: {OUT_CSV}  ({OUT_CSV.stat().st_size // 1024:,} KB)")
        return

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Downloading Divvy Jan 2024 from:\n  {SOURCE_URL}")
    with urllib.request.urlopen(SOURCE_URL) as resp:
        data = resp.read()

    with zipfile.ZipFile(io.BytesIO(data)) as z:
        csv_name = next(n for n in z.namelist() if n.endswith(".csv"))
        with z.open(csv_name) as src, open(OUT_CSV, "wb") as dst:
            shutil.copyfileobj(src, dst)

    print(f"Saved: {OUT_CSV}  ({OUT_CSV.stat().st_size // 1024:,} KB)")
    print(f"Citation: {SOURCE_CITATION}")


if __name__ == "__main__":
    download()
