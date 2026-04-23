from __future__ import annotations

import argparse
import json
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "data" / "raw" / "nyc_taxi"

TRIP_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def download(url: str, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as response, target.open("wb") as sink:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            sink.write(chunk)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    trip_path = RAW_DIR / "yellow_tripdata_2024-01.parquet"
    zone_path = RAW_DIR / "taxi_zone_lookup.csv"

    if args.force or not trip_path.exists():
        download(TRIP_URL, trip_path)
    if args.force or not zone_path.exists():
        download(ZONE_URL, zone_path)

    print(
        json.dumps(
            {
                "trip_path": str(trip_path),
                "trip_size_bytes": trip_path.stat().st_size,
                "zone_path": str(zone_path),
                "zone_size_bytes": zone_path.stat().st_size,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
