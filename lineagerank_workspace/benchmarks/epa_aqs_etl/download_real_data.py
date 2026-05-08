"""Download EPA AQS PM2.5 daily summary data (2023) and site metadata.

Sources:
    US EPA Air Quality System (AQS) — PM2.5 FRM/FEM daily summaries, 2023
    https://aqs.epa.gov/aqsweb/airdata/daily_88101_2023.zip
    Parameter code 88101 = PM2.5 - Local Conditions (FRM/FEM)

    AQS site metadata (all monitoring sites, stable URL)
    https://aqs.epa.gov/aqsweb/airdata/aqs_sites.zip

    ~600,000 site-day observations across the US for 2023.

Citation:
    U.S. Environmental Protection Agency, "Air Quality System (AQS) Daily Summary
    Data — PM2.5 FRM/FEM, 2023," EPA Air Quality System, 2024.
    Available: https://aqs.epa.gov/aqsweb/airdata/download_files.html
    Public domain — U.S. government work.
"""
from __future__ import annotations

import io
import shutil
import urllib.request
import zipfile
from pathlib import Path

ROOT    = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "data" / "raw" / "epa_aqs"

_MEAS_URL  = "https://aqs.epa.gov/aqsweb/airdata/daily_88101_2023.zip"
_SITES_URL = "https://aqs.epa.gov/aqsweb/airdata/aqs_sites.zip"

MEAS_CSV  = OUT_DIR / "daily_88101_2023.csv"
SITES_CSV = OUT_DIR / "aqs_sites.csv"


def _download_zip(url: str, dest_csv: Path, label: str) -> None:
    print(f"Downloading {label} from:\n  {url}")
    with urllib.request.urlopen(url, timeout=120) as resp:
        data = resp.read()
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        csv_name = next(n for n in z.namelist() if n.endswith(".csv"))
        with z.open(csv_name) as src, open(dest_csv, "wb") as dst:
            shutil.copyfileobj(src, dst)
    print(f"Saved: {dest_csv}  ({dest_csv.stat().st_size // 1024:,} KB)")


def download() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if MEAS_CSV.exists():
        print(f"Already present: {MEAS_CSV.name}  ({MEAS_CSV.stat().st_size // 1024:,} KB)")
    else:
        _download_zip(_MEAS_URL, MEAS_CSV, "EPA AQS PM2.5 daily 2023")

    if SITES_CSV.exists():
        print(f"Already present: {SITES_CSV.name}  ({SITES_CSV.stat().st_size // 1024:,} KB)")
    else:
        _download_zip(_SITES_URL, SITES_CSV, "EPA AQS site metadata")


if __name__ == "__main__":
    download()
