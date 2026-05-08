"""Download Chicago Crime 2023 data via City of Chicago Open Data (Socrata).

Sources:
    City of Chicago Data Portal — Crimes 2001 to Present
    https://data.cityofchicago.org/resource/ijzp-q8t2.csv (filtered to 2023)

    IUCR Code Lookup (Illinois Uniform Crime Reporting codes)
    https://data.cityofchicago.org/resource/c7ck-438e.csv

    ~264,000 crime reports in Chicago for calendar year 2023.

Citation:
    City of Chicago, "Crimes — 2001 to Present," Chicago Data Portal, 2024.
    Available: https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2
    Public domain — City of Chicago open data.
"""
from __future__ import annotations

import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT         = Path(__file__).resolve().parents[2]
OUT_DIR      = ROOT / "data" / "raw" / "chicago_crime"
CRIMES_CSV   = OUT_DIR / "crimes_2023.csv"
IUCR_CSV     = OUT_DIR / "iucr_codes.csv"

_CRIMES_BASE = "https://data.cityofchicago.org/resource/ijzp-q8t2.csv"
_IUCR_URL    = "https://data.cityofchicago.org/resource/c7ck-438e.csv?$limit=1000"
_PAGE_SIZE   = 50_000
_TARGET_ROWS = 300_000  # cap — actual 2023 count is ~264k


def _fetch_crimes() -> None:
    """Download crimes filtered to year=2023 via Socrata offset pagination."""
    columns = (
        "id,date,primary_type,description,location_description,"
        "arrest,domestic,beat,district,ward,community_area,fbi_code,"
        "x_coordinate,y_coordinate,year,latitude,longitude,iucr"
    )
    rows_written = 0
    header_written = False

    with open(CRIMES_CSV, "w", encoding="utf-8") as out_f:
        offset = 0
        while True:
            params = urllib.parse.urlencode({
                "$where":  "year=2023",
                "$select": columns,
                "$limit":  str(_PAGE_SIZE),
                "$offset": str(offset),
                "$order":  "id",
            })
            url = f"{_CRIMES_BASE}?{params}"
            for attempt in range(3):
                try:
                    with urllib.request.urlopen(url, timeout=60) as resp:
                        page = resp.read().decode("utf-8")
                    break
                except urllib.error.URLError:
                    if attempt == 2:
                        raise
                    time.sleep(2 ** attempt)

            lines = page.strip().splitlines()
            if len(lines) <= 1:
                break  # only header or empty — done

            if not header_written:
                out_f.write(lines[0] + "\n")
                header_written = True
                data_lines = lines[1:]
            else:
                data_lines = lines[1:]

            for line in data_lines:
                out_f.write(line + "\n")

            rows_written += len(data_lines)
            print(f"  crimes_2023: {rows_written:,} rows …", end="\r")

            if len(data_lines) < _PAGE_SIZE or rows_written >= _TARGET_ROWS:
                break
            offset += _PAGE_SIZE

    print(f"\nSaved: {CRIMES_CSV}  ({CRIMES_CSV.stat().st_size // 1024:,} KB, {rows_written:,} rows)")


def download() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if CRIMES_CSV.exists() and IUCR_CSV.exists():
        print(f"Already present: {CRIMES_CSV.name}  ({CRIMES_CSV.stat().st_size // 1024:,} KB)")
        print(f"Already present: {IUCR_CSV.name}  ({IUCR_CSV.stat().st_size // 1024:,} KB)")
        return

    if not CRIMES_CSV.exists():
        print(f"Downloading Chicago Crime 2023 from Socrata …")
        _fetch_crimes()

    if not IUCR_CSV.exists():
        print(f"Downloading IUCR lookup from:\n  {_IUCR_URL}")
        with urllib.request.urlopen(_IUCR_URL, timeout=30) as resp:
            IUCR_CSV.write_bytes(resp.read())
        print(f"Saved: {IUCR_CSV}  ({IUCR_CSV.stat().st_size // 1024:,} KB)")


if __name__ == "__main__":
    download()
