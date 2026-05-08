"""Download NOAA Storm Events 2023 data.

Source:
    NOAA National Centers for Environmental Information (NCEI)
    Storm Events Database — Details and Fatalities files for 2023
    https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/

    Filenames have a release-date suffix that changes with data updates, so
    this script lists the directory and picks the latest 2023 file for each type.

    ~100,000 storm events and ~700 fatalities in the US for 2023.

Citation:
    NOAA National Centers for Environmental Information, "Storm Events Database,"
    National Oceanic and Atmospheric Administration, 2024.
    Available: https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/
    Public domain — U.S. government work.
"""
from __future__ import annotations

import gzip
import io
import re
import shutil
import urllib.request
from pathlib import Path

ROOT    = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "data" / "raw" / "noaa_storm"

EVENTS_CSV     = OUT_DIR / "storm_events_2023.csv"
FATALITIES_CSV = OUT_DIR / "storm_fatalities_2023.csv"

_INDEX_URL = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"


def _discover_url(pattern: str) -> str:
    """List the NOAA directory and return the URL of the latest matching file."""
    with urllib.request.urlopen(_INDEX_URL, timeout=30) as resp:
        html = resp.read().decode("utf-8")
    matches = sorted(re.findall(pattern, html))
    if not matches:
        raise FileNotFoundError(f"No file matching {pattern!r} found at {_INDEX_URL}")
    filename = matches[-1]  # latest release date sorts last
    return _INDEX_URL + filename


def _download_gz(url: str, dest: Path, label: str) -> None:
    print(f"Downloading {label} from:\n  {url}")
    with urllib.request.urlopen(url, timeout=120) as resp:
        compressed = resp.read()
    with gzip.open(io.BytesIO(compressed)) as gz_f:
        with open(dest, "wb") as out_f:
            shutil.copyfileobj(gz_f, out_f)
    print(f"Saved: {dest}  ({dest.stat().st_size // 1024:,} KB)")


def download() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if EVENTS_CSV.exists() and FATALITIES_CSV.exists():
        print(f"Already present: {EVENTS_CSV.name}  ({EVENTS_CSV.stat().st_size // 1024:,} KB)")
        print(f"Already present: {FATALITIES_CSV.name}  ({FATALITIES_CSV.stat().st_size // 1024:,} KB)")
        return

    det_pattern = r"StormEvents_details-ftp_v1\.0_d2023_c\d+\.csv\.gz"
    fat_pattern = r"StormEvents_fatalities-ftp_v1\.0_d2023_c\d+\.csv\.gz"

    if not EVENTS_CSV.exists():
        url = _discover_url(det_pattern)
        _download_gz(url, EVENTS_CSV, "NOAA Storm Events details 2023")

    if not FATALITIES_CSV.exists():
        url = _discover_url(fat_pattern)
        _download_gz(url, FATALITIES_CSV, "NOAA Storm Events fatalities 2023")


if __name__ == "__main__":
    download()
