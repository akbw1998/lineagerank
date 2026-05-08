"""Download NHTSA FARS 2022 fatal crash data.

Source:
    National Highway Traffic Safety Administration (NHTSA)
    Fatality Analysis Reporting System (FARS), 2022 Annual Report File
    https://static.nhtsa.gov/nhtsa/downloads/FARS/2022/National/FARS2022NationalCSV.zip

Tables used (extracted from ZIP):
    ACCIDENT.CSV  — one row per crash (~42,795 crashes)
    VEHICLE.CSV   — one row per vehicle involved (~77,000 vehicles)
    PERSON.CSV    — one row per person involved (~55,000 persons)
    All three join on ST_CASE (state case number).

Citation:
    National Highway Traffic Safety Administration, "Fatality Analysis Reporting
    System (FARS) 2022 Annual Report File," U.S. Department of Transportation,
    2023. Available: https://www.nhtsa.gov/research-data/fatality-analysis-reporting-system-fars
    Public domain — U.S. government work.
"""
from __future__ import annotations

import io
import shutil
import urllib.request
import zipfile
from pathlib import Path

SOURCE_URL = (
    "https://static.nhtsa.gov/nhtsa/downloads/FARS/2022/National/FARS2022NationalCSV.zip"
)

ROOT    = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "data" / "raw" / "nhtsa_fars"
NEEDED  = ["ACCIDENT.CSV", "VEHICLE.CSV", "PERSON.CSV"]


def download() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if all((OUT_DIR / f).exists() for f in NEEDED):
        for f in NEEDED:
            size = (OUT_DIR / f).stat().st_size // 1024
            print(f"Already present: {f}  ({size:,} KB)")
        return

    print(f"Downloading NHTSA FARS 2022 from:\n  {SOURCE_URL}")
    with urllib.request.urlopen(SOURCE_URL, timeout=120) as resp:
        data = resp.read()

    with zipfile.ZipFile(io.BytesIO(data)) as z:
        names = z.namelist()
        for target in NEEDED:
            # Files may be in a subdirectory inside the ZIP
            matches = [n for n in names if n.upper().endswith(target)]
            if not matches:
                raise FileNotFoundError(f"{target} not found in ZIP. Available: {names[:10]}")
            src_name = matches[0]
            dest = OUT_DIR / target
            with z.open(src_name) as src, open(dest, "wb") as dst:
                shutil.copyfileobj(src, dst)
            print(f"Extracted: {dest}  ({dest.stat().st_size // 1024:,} KB)")

    print("NHTSA FARS 2022 download complete.")


if __name__ == "__main__":
    download()
