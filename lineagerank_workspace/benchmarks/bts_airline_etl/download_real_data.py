"""Download BTS Airline On-Time Performance data (Jan 2024).

Source:
    U.S. Bureau of Transportation Statistics (BTS) — Airline On-Time Performance Data
    https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FGK
    Direct ZIP: https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_1.zip

Dataset citation:
    U.S. Bureau of Transportation Statistics, "Airline On-Time Performance Data,"
    Reporting Carrier On-Time Performance (1987–present), January 2024.
    Available: https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_1.zip
    Public domain — U.S. government work.

Key columns used:
    FlightDate, Reporting_Airline (carrier), Origin, OriginState, OriginStateName,
    Dest, DepDelay, ArrDelay, Distance, Cancelled

Row count (Jan 2024): 547,271 flights
"""
from __future__ import annotations

import io
import shutil
import urllib.request
import zipfile
from pathlib import Path

SOURCE_URL = (
    "https://transtats.bts.gov/PREZIP/"
    "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_1.zip"
)
SOURCE_CITATION = (
    'U.S. Bureau of Transportation Statistics, "Airline On-Time Performance Data," '
    "Reporting Carrier On-Time Performance (1987–present), January 2024. "
    "Available: https://transtats.bts.gov/PREZIP/"
    "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_1.zip"
)

ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "data" / "raw" / "bts_airline"
OUT_CSV = OUT_DIR / "On_Time_2024_1.csv"


def download() -> None:
    if OUT_CSV.exists():
        print(f"Already present: {OUT_CSV}  ({OUT_CSV.stat().st_size // 1024:,} KB)")
        return

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Downloading BTS On-Time Jan 2024 from:\n  {SOURCE_URL}")
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
