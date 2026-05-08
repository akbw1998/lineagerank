# PipeRCA-Bench Expansion Plan: 360 → 720 Incidents

## Goal

Expand PipeRCA-Bench from 360 incidents (4 pipelines) to 720 incidents (8 pipelines) by adding 4 new real-world public-dataset pipeline families, each with a topologically distinct DAG structure not present in the existing 4 pipelines.

Target: ~720–750 incidents (8 pipelines × 6 faults × 15 iterations = 720), making it comparable to the microservice RCA benchmarks cited in the paper (~735 incidents in RCAEval).

---

## Current State (4 pipelines, 360 incidents)

| Pipeline | Dataset | Topology | Nodes | Seed |
|----------|---------|----------|-------|------|
| nyc_yellow_taxi_etl_real | NYC TLC Yellow Jan 2024 (120k rows) | Fan-out chain: 1 join → 3 leaf marts | 8 | 1337 |
| nyc_green_taxi_etl_real | NYC TLC Green Jan 2024 (56k rows) | Same topology as Yellow | 8 | 9999 |
| divvy_chicago_bike_real | Divvy Bike Jan 2024 (120k rows) | Fan-out chain: 1 join → 3 leaf marts | 8 | 7777 |
| bts_airline_ontime_real | BTS Airline Jan 2024 (120k rows) | Dual-path DAG: 1 dim joins 2 nodes | 8 | 3141 |

Incident structure: 4 pipelines × 6 faults × 15 iterations = 360, balanced 120 each across full/sparse/missing-root observability.

---

## 4 New Pipelines to Add

### Pipeline 5: NHTSA FARS 2022 — **3-source fan-in**

**Topology novelty**: FIRST pipeline with 3 true source nodes merging at one staging node (vs. max 2 sources in all existing pipelines).

**Dataset**: National Highway Traffic Safety Administration Fatal Accident Reporting System 2022
- Download URL: `https://static.nhtsa.gov/nhtsa/downloads/FARS/2022/National/FARS2022NationalCSV.zip`
- Tables used: `ACCIDENT.CSV` (~42k rows), `VEHICLE.CSV` (~77k rows), `PERSON.CSV` (~55k rows)
- All join on `ST_CASE` (state case number)
- Public domain (US government)

**8 Nodes**:
1. `raw_accidents` (source) — crash-level data: ST_CASE, STATENAME, FATALS, DRUNK_DR, DAY, MONTH, HOUR
2. `raw_vehicles` (source) — vehicle-level: ST_CASE, VEH_NO, MAKENAME, BODY_TYPNAME, SPEEDREL
3. `raw_persons` (source) — person-level: ST_CASE, PER_NO, AGE, SEX, INJ_SEV, SEAT_POS
4. `crashes_valid` (staging) — filter accidents with valid state/date/hour
5. `crash_merged` (staging) — 3-source merge: join crashes_valid + agg from raw_vehicles + agg from raw_persons on ST_CASE
6. `crash_classified` (staging) — severity classification: fatal/serious/minor
7. `daily_crash_metrics` (mart leaf A) — time-series: crashes per day, avg fatalities
8. `state_crash_summary` (mart leaf B) — geographic: crashes by state, drunk-driving rates

**Edges**:
```
raw_accidents  → crashes_valid
raw_vehicles   → crash_merged   (3-way fan-in)
raw_persons    → crash_merged   (3-way fan-in)
crashes_valid  → crash_merged   (3-way fan-in)
crash_merged   → crash_classified
crash_classified → daily_crash_metrics
crash_classified → state_crash_summary
```

**PipelineSpec fields**:
- `source_nodes`: `("raw_accidents", "raw_vehicles", "raw_persons")`
- `join_nodes`: `("crash_merged",)` — the 3-source merge point
- `leaf_test_weights`: `{"daily_crash_metrics": 3, "state_crash_summary": 3}`

**Fault mappings**:
- `missing_partition`: delete most-populated MONTH from raw_accidents → `daily_crash_metrics` observed
- `duplicate_ingestion`: duplicate most-populated MONTH → `daily_crash_metrics` observed
- `stale_source`: drop 40% most-recent rows from raw_accidents → `daily_crash_metrics` observed
- `null_explosion`: nullify ~1/7 HOUR values in raw_accidents → `crash_merged` observed
- `bad_join_key`: corrupt raw_vehicles (shift VEH_NO values), breaking the merge → `crash_merged` observed
- `schema_drift`: hardcode severity_class='fatal' in crash_classified → `state_crash_summary` observed

**Download script**: `benchmarks/nhtsa_fars_etl/download_real_data.py`
- Downloads and unzips the FARS2022 ZIP
- Extracts ACCIDENT.CSV, VEHICLE.CSV, PERSON.CSV to `data/raw/nhtsa_fars/`

**Seed base**: 2468 (distinct from all existing)

**Signal augmentation additions to `_build_signals`**:
- `stale_source`: add `"daily_crash_metrics" in signals` block
- `missing_partition`: add `"daily_crash_metrics" in signals` block
- `duplicate_ingestion`: add `"state_crash_summary" in signals` block (revenue-analog)
- `null_explosion`: add `"crash_merged" in signals` for enrichment-node block
- `bad_join_key`: add `"crash_merged" in signals` for enrichment-node block (unknown_delta uses a `crashed_vehicle_missing` validation counter)
- `schema_drift`: add `"state_crash_summary" in signals` for classification-leaf block

---

### Pipeline 6: Chicago Crime 2023 — **Diamond (parallel enrichment)**

**Topology novelty**: FIRST pipeline with two parallel processing branches that RECONVERGE at a merge node (diamond pattern). No existing pipeline has this structure.

**Dataset**: City of Chicago Open Data — Crimes dataset, filtered to 2023
- Download URL: `https://data.cityofchicago.org/resource/ijzp-q8t2.csv?$where=year%3D2023&$limit=300000`
- Plus IUCR lookup: `https://data.cityofchicago.org/resource/c7ck-438e.csv` (crime type codes, ~400 rows)
- ~264k crimes in Chicago 2023
- Public (City of Chicago Open Data)

**8 Nodes**:
1. `raw_crimes` (source) — crime reports: id, date, primary_type, description, beat, district, ward, latitude, longitude, iucr
2. `iucr_lookup` (source) — static crime type lookup: iucr code → primary_description, secondary_description, index_code
3. `crimes_valid` (staging) — filter crimes with valid date/location/type
4. `type_enriched` (staging, branch A) — join iucr_lookup for full type descriptions
5. `district_enriched` (staging, branch B) — aggregate district-level context from crimes_valid
6. `crimes_unified` (mart) — merge branches A+B (join type_enriched + district_enriched on id/district)
7. `daily_crime_metrics` (mart leaf A) — time-series: crimes per day by type
8. `district_summary` (mart leaf B) — geographic: arrest rates, top types by district

**Edges**:
```
raw_crimes   → crimes_valid
iucr_lookup  → type_enriched   (parallel branch A)
crimes_valid → type_enriched   (parallel branch A)
crimes_valid → district_enriched  (parallel branch B)
type_enriched    → crimes_unified  (reconvergence)
district_enriched → crimes_unified  (reconvergence)
crimes_unified → daily_crime_metrics
crimes_unified → district_summary
```

**PipelineSpec fields**:
- `source_nodes`: `("raw_crimes", "iucr_lookup")`
- `join_nodes`: `("type_enriched", "crimes_unified")` — two join points
- `leaf_test_weights`: `{"daily_crime_metrics": 3, "district_summary": 3}`

**Fault mappings**:
- `missing_partition`: delete most-populated month from raw_crimes → `daily_crime_metrics` observed
- `duplicate_ingestion`: duplicate most-populated month → `district_summary` observed
- `stale_source`: drop 40% most-recent rows from raw_crimes → `daily_crime_metrics` observed
- `null_explosion`: nullify ~1/7 district values in raw_crimes → `district_enriched` observed
- `bad_join_key`: corrupt iucr_lookup (shift iucr codes), breaking type join → `type_enriched` observed
- `schema_drift`: hardcode crime_severity='low' in crimes_unified → `district_summary` observed

**Download script**: `benchmarks/chicago_crime_etl/download_real_data.py`
- Downloads crimes CSV via Socrata API (with retry/pagination for 300k limit)
- Downloads IUCR lookup CSV
- Saves to `data/raw/chicago_crime/`

**Seed base**: 5555 (distinct from all existing)

**Signal augmentation additions to `_build_signals`**:
- `stale_source`/`missing_partition`: add `"daily_crime_metrics" in signals` block
- `duplicate_ingestion`: add `"district_summary" in signals` block
- `null_explosion`: add `"district_enriched" in signals` (enrichment-node block)
- `bad_join_key`: add `"type_enriched" in signals` (enrichment-node block); `unknown_delta` uses `missing_type_count` validation counter
- `schema_drift`: add `"district_summary" in signals` block

---

### Pipeline 7: EPA AQS PM2.5 Daily 2023 — **Deep temporal aggregation chain**

**Topology novelty**: Deepest aggregation chain (daily → state-monthly → national), with a lateral branch to exceedance flagging. Introduces temporal hierarchy aggregation absent from existing pipelines.

**Dataset**: US EPA Air Quality System (AQS) — PM2.5 daily summaries 2023
- Download URL: `https://aqs.epa.gov/aqsweb/airdata/daily_88101_2023.zip` (~600k measurement days)
- Site metadata: `https://aqs.epa.gov/aqsweb/airdata/aqs_sites.zip` (stable URL, ~25k sites)
- Public domain (US EPA)

**8 Nodes**:
1. `raw_measurements` (source) — daily PM2.5 observations: site_id, date, arithmetic_mean, aqi, state_code, county_code
2. `site_lookup` (source) — station metadata: site_id, state_name, county_name, latitude, longitude, elevation
3. `measurements_valid` (staging) — filter valid AQI range, non-null site
4. `site_enriched` (staging) — join site_lookup: add state_name, county_name, elevation
5. `daily_county` (staging) — aggregate to county-day: mean PM2.5, max AQI, site_count
6. `state_monthly` (mart) — aggregate county-day to state-month: mean, max, days_exceeding_standard
7. `exceedance_sites` (mart leaf A) — flag sites exceeding EPA 24hr standard (35 µg/m³)
8. `national_summary` (mart leaf B) — national statistics: pct of days exceeding, worst states

**Edges**:
```
raw_measurements → measurements_valid
site_lookup      → site_enriched
measurements_valid → site_enriched
site_enriched    → daily_county
daily_county     → state_monthly
daily_county     → exceedance_sites
state_monthly    → national_summary
```

**PipelineSpec fields**:
- `source_nodes`: `("raw_measurements", "site_lookup")`
- `join_nodes`: `("site_enriched",)`
- `leaf_test_weights`: `{"exceedance_sites": 3, "national_summary": 3}`

**Fault mappings**:
- `missing_partition`: delete most-populated month from raw_measurements → `state_monthly` observed
- `duplicate_ingestion`: duplicate most-populated month → `national_summary` observed
- `stale_source`: drop 40% most-recent rows from raw_measurements → `state_monthly` observed
- `null_explosion`: nullify ~1/7 aqi values in raw_measurements → `site_enriched` observed
- `bad_join_key`: corrupt site_lookup site_ids → `site_enriched` observed; `unknown_delta` = missing site count
- `schema_drift`: hardcode aqi_category='Good' in daily_county → `exceedance_sites` observed

**Download script**: `benchmarks/epa_aqs_etl/download_real_data.py`
- Downloads and unzips daily_88101_2023.zip → `data/raw/epa_aqs/daily_88101_2023.csv`
- Downloads and unzips aqs_sites.zip → `data/raw/epa_aqs/aqs_sites.csv`

**Seed base**: 6174 (distinct from all existing; Kaprekar's constant)

**Signal augmentation additions to `_build_signals`**:
- `stale_source`/`missing_partition`: add `"state_monthly" in signals` block (time-series primary)
- `duplicate_ingestion`: add `"national_summary" in signals` block
- `null_explosion`: add `"site_enriched" in signals` block (enrichment-node); `"exceedance_sites" in signals` for leaf
- `bad_join_key`: add `"site_enriched" in signals`; validation counter = `missing_site_count`
- `schema_drift`: add `"exceedance_sites" in signals` block; `"national_summary" in signals` for sibling

---

### Pipeline 8: NOAA Storm Events 2023 — **Wide fan-out (3 leaf nodes, 2 sources)**

**Topology novelty**: 3 leaf mart nodes branching from a single parent (same pattern as Yellow/Green/Divvy but different domain, deeper chain, and with a true 2-table source join on EVENT_ID). Adds a 3rd leaf to increase topological scale.

**Dataset**: NOAA National Centers for Environmental Information — Storm Events Database 2023
- Details file: scan `https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/` for `StormEvents_details-ftp_v1.0_d2023_c*.csv.gz` (filename has release-date suffix, script must discover latest)
- Fatalities file: same directory, `StormEvents_fatalities-ftp_v1.0_d2023_c*.csv.gz`
- ~100k storm events, ~1.5k fatalities in 2023
- Public domain (NOAA)

**8 Nodes**:
1. `raw_events` (source) — event-level: event_id, state, begin_date_time, event_type, magnitude, injuries_direct, deaths_direct, damage_property
2. `raw_fatalities` (source) — fatality-level: fatality_id, event_id, fatality_type, fatality_date, fatality_age, fatality_sex
3. `events_valid` (staging) — filter events with valid state/date/type
4. `fatality_enriched` (staging) — join raw_fatalities to events_valid on event_id
5. `events_classified` (staging) — classify severity: catastrophic/severe/moderate/minor
6. `daily_event_metrics` (mart leaf A) — time-series: events per day by type
7. `state_summary` (mart leaf B) — geographic: events/fatalities/damage by state
8. `damage_distribution` (mart leaf C) — economic: property damage distribution by event_type

**Edges**:
```
raw_events      → events_valid
raw_fatalities  → fatality_enriched
events_valid    → fatality_enriched
fatality_enriched → events_classified
events_classified → daily_event_metrics
events_classified → state_summary
events_classified → damage_distribution
```

**PipelineSpec fields**:
- `source_nodes`: `("raw_events", "raw_fatalities")`
- `join_nodes`: `("fatality_enriched",)`
- `leaf_test_weights`: `{"daily_event_metrics": 2, "state_summary": 2, "damage_distribution": 2}`

**Fault mappings**:
- `missing_partition`: delete most-populated month from raw_events → `daily_event_metrics` observed
- `duplicate_ingestion`: duplicate most-populated month → `damage_distribution` observed
- `stale_source`: drop 40% most-recent rows from raw_events → `daily_event_metrics` observed
- `null_explosion`: nullify ~1/7 event_type values in raw_events → `fatality_enriched` observed
- `bad_join_key`: corrupt raw_fatalities event_id values → `fatality_enriched` observed
- `schema_drift`: hardcode severity_class='moderate' in events_classified → `damage_distribution` observed

**Download script**: `benchmarks/noaa_storm_etl/download_real_data.py`
- Lists NOAA CSV directory via HTTP, finds latest 2023 details + fatalities files
- Downloads + gunzips to `data/raw/noaa_storm/`

**Seed base**: 8008 (distinct from all existing)

**Signal augmentation additions to `_build_signals`**:
- `stale_source`/`missing_partition`: add `"daily_event_metrics" in signals` block
- `duplicate_ingestion`: add `"damage_distribution" in signals` block
- `null_explosion`: add `"fatality_enriched" in signals` (enrichment-node); `"daily_event_metrics" in signals` for leaf
- `bad_join_key`: add `"fatality_enriched" in signals`; validation counter = `missing_fatality_count`
- `schema_drift`: add `"damage_distribution" in signals` and `"state_summary" in signals` blocks

---

## Implementation Steps (in order)

### Step 1: Create 4 download scripts
Files to create:
- `benchmarks/nhtsa_fars_etl/download_real_data.py`
- `benchmarks/chicago_crime_etl/download_real_data.py`
- `benchmarks/epa_aqs_etl/download_real_data.py`
- `benchmarks/noaa_storm_etl/download_real_data.py`

Each script saves data to `data/raw/{name}/`.

### Step 2: Add 4 PipelineSpecs to `rca_benchmark.py`
Add to `get_pipeline_specs()` dict: `"nhtsa_fars_crash"`, `"chicago_crime_2023"`, `"epa_aqs_pm25"`, `"noaa_storm_events"`.
Follow exact same `PipelineSpec(name, nodes, edges, source_nodes, transform_nodes, join_nodes, leaf_test_weights)` pattern.

### Step 3: Add pipeline functions to `run_real_case_study.py`
For each of the 4 new pipelines, add 3 functions following the exact naming pattern:

```python
def _load_{prefix}_sources(conn, max_rows): ...  # reads from data/raw/{name}/
def _run_{prefix}_pipeline(conn, schema_drifted=False): ...  # SQL steps + lineage
def _apply_{prefix}_fault(conn, fault_type, iteration): ...  # returns (root, observed, schema_drifted)
```

SQL steps follow same pattern as existing pipelines:
- Constants for each SQL block (e.g., `_NHTSA_SQL_VALID`, `_NHTSA_SQL_MERGED`, etc.)
- `_run()` helper inside `_run_*_pipeline` that executes SQL, records row count, records lineage

### Step 4: Update `_build_signals` in `run_real_case_study.py`
Add new node name checks to the 5 fault-type signal augmentation blocks (stale_source, missing_partition, duplicate_ingestion, null_explosion, bad_join_key, schema_drift). Uses the same `if "node_name" in signals:` pattern — no branching on pipeline name.

For bad_join_key, each new pipeline needs a validation counter (like `unknown_borough`). The `_run_*_pipeline` functions must return the right key in their `validations` dict, and `_build_signals` must know which key to use. Look at how `faulted_val.get("unknown_borough", 0)` is used — new pipelines will add `"crashed_vehicle_missing"`, `"missing_type_count"`, `"missing_site_count"`, `"missing_fatality_count"`.

### Step 5: Wire up new pipelines in `run_real_case_study.py` `main()`
After the BTS block (line ~2420), add 4 more blocks:
```python
has_nhtsa = (ROOT / "data" / "raw" / "nhtsa_fars" / "ACCIDENT.CSV").exists()
has_chicago = (ROOT / "data" / "raw" / "chicago_crime" / "crimes_2023.csv").exists()
has_epa = (ROOT / "data" / "raw" / "epa_aqs" / "daily_88101_2023.csv").exists()
has_noaa = (ROOT / "data" / "raw" / "noaa_storm" / "storm_events_2023.csv").exists()
```

Each block follows the `_run_one_pipeline(...)` call pattern with new spec, load_fn, run_fn, fault_fn, pipeline_name, seed_base.

Update `active_pipelines` list, `data_source_parts`, and summary `incident_count` fields.

Update the `_build_incident` docstring string for `pipeline` field to include new pipeline names.

### Step 6: Update `run_strengthened_suite.py`
Add 4 `_download_if_missing()` calls for the new data files.

### Step 7: Update `run_real_case_study.py` module docstring
Change "Four real public datasets" to "Eight real public datasets", update total incident count to 720.

### Step 8: Update paper draft `docs/lineagerank_v10_draft.md`
- Abstract: 360 → 720 incidents, "four" → "eight" pipeline families
- Benchmark count: 360 → 720
- Table I comparison: update PipeRCA-Bench row (360 → 720)
- §V.A Pipeline Families: add descriptions of 4 new families + update topology diagrams
- §VIII Conclusion: update counts
- Regenerate PDF

---

## Key Files to Modify

| File | Change |
|------|--------|
| `tools/rca_benchmark.py` | Add 4 PipelineSpecs to `get_pipeline_specs()` |
| `tools/run_real_case_study.py` | Add load/run/fault functions + update `_build_signals` + update `main()` |
| `tools/run_strengthened_suite.py` | Add 4 download checks |
| `docs/lineagerank_v10_draft.md` | Update counts and pipeline family descriptions |
| **New**: `benchmarks/nhtsa_fars_etl/download_real_data.py` | NHTSA download |
| **New**: `benchmarks/chicago_crime_etl/download_real_data.py` | Chicago crime download |
| **New**: `benchmarks/epa_aqs_etl/download_real_data.py` | EPA AQS download |
| **New**: `benchmarks/noaa_storm_etl/download_real_data.py` | NOAA Storm Events download |

---

## Codebase Patterns (critical to follow exactly)

### PipelineSpec (in `rca_benchmark.py`)
```python
from dataclasses import dataclass
from typing import Iterable
import networkx as nx

@dataclass
class PipelineSpec:
    name: str
    nodes: dict[str, str]            # node_name -> "source"|"staging"|"mart"
    edges: list[tuple[str, str]]     # (parent, child)
    source_nodes: tuple[str, ...]
    transform_nodes: tuple[str, ...]
    join_nodes: tuple[str, ...]
    leaf_test_weights: dict[str, int]
```

### _run_*_pipeline return dict
```python
return {
    "runtime_lineage": lineage,   # list of (src, dst) tuples from _run() calls
    "step_rows": step_rows,       # node_name -> row count (including source tables counted first!)
    "validations": {
        "unknown_X": int(val),    # key must match what _build_signals uses for bad_join_key delta
        "daily_X_rows": int(val),
        "Y_rows": int(val),
    },
}
```
**Critical**: Source tables must be counted in `step_rows` BEFORE any SQL runs, so that row_delta for Type A faults is non-zero at source nodes.

### _apply_*_fault return tuple
```python
return (root_node_name, observed_failure_node_name, schema_drifted_bool)
```

### _build_signals auto-detection
The function auto-detects which spec to use by matching `set(s.nodes.keys()) == set(spec_nodes.keys())`. New PipelineSpecs MUST be registered in `get_pipeline_specs()` for this to work.

### Seed bases (must all be distinct)
- Yellow: 1337, Green: 9999, Divvy: 7777, BTS: 3141
- NHTSA: 2468, Chicago: 5555, EPA: 6174, NOAA: 8008

### Observability rotation (same for all pipelines, in `_run_one_pipeline`)
- Iterations 0–4: full
- Iterations 5–9: sparse (30% edge drop)
- Iterations 10–14: missing-root (all root outgoing edges removed)

---

## Topology Comparison (existing + new)

| Pipeline | Source nodes | Join nodes | Leaf nodes | Topology name |
|----------|-------------|-----------|-----------|---------------|
| Yellow Taxi | 2 | 1 | 3 | Fan-out chain |
| Green Taxi | 2 | 1 | 3 | Fan-out chain (same) |
| Divvy Bike | 2 | 1 | 3 | Fan-out chain |
| BTS Airline | 2 | 2 | 3 | Dual-path DAG |
| **NHTSA FARS** | **3** | **1** | **2** | **3-source fan-in** |
| **Chicago Crime** | **2** | **2** | **2** | **Diamond (parallel reconvergence)** |
| **EPA AQS** | **2** | **1** | **2** | **Deep temporal chain** |
| **NOAA Storm** | **2** | **1** | **3** | **Wide fan-out (3 leaves)** |

---

## Verification After Implementation

1. Run download scripts (or test with stub data): check files appear in `data/raw/`
2. Quick heuristics run (no LLM): `PYTHONUNBUFFERED=1 /opt/venv/bin/python3 tools/run_strengthened_suite.py --no-lrllm`
3. Confirm output JSON has `incident_count: 720`
4. Confirm `by_pipeline` summary shows 8 keys with 90 incidents each
5. Verify `by_observability` totals: full=240, sparse=240, missing_root=240
6. Run full LLM suite if needed: omit `--no-lrllm`
7. Export updated PDF after updating paper draft

---

## Notes and Caveats

- NOAA Storm Events download URL has a date-dependent filename suffix. The download script must LIST the directory HTML and regex-match the latest 2023 file (see implementation note below).
- Chicago Crime download via Socrata may need retry logic for 300k row limit — use `$offset` pagination if the API returns fewer rows than expected.
- EPA AQS PM2.5 (`daily_88101_2023.zip`) contains only PM2.5 FRM/FEM monitors; the site file (`aqs_sites.zip`) has all sites. The join key is a composite: State Code + County Code + Site Number. Need to construct a consistent site_id string for joining.
- NHTSA FARS ZIP file structure: the 2022 ZIP contains files in a flat structure (`ACCIDENT.CSV`, `VEHICLE.CSV`, `PERSON.CSV` in uppercase). The 2022 data is final/complete; 2023 data might still be preliminary as of mid-2025.
- For `bad_join_key` fault in NHTSA: corrupting `raw_vehicles` VEH_NO values will cause fewer vehicle records to match when aggregating per crash. The validation counter (`crashed_vehicle_missing`) should count crashes in `crash_merged` that have 0 matched vehicles vs baseline.

---

## NOAA Download URL Discovery Pattern

```python
import urllib.request, re

INDEX_URL = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
html = urllib.request.urlopen(INDEX_URL).read().decode()

# Find latest 2023 details file
det_pattern = r"StormEvents_details-ftp_v1\.0_d2023_c\d+\.csv\.gz"
det_files = sorted(re.findall(det_pattern, html))
details_file = det_files[-1]  # latest release

# Find latest 2023 fatalities file  
fat_pattern = r"StormEvents_fatalities-ftp_v1\.0_d2023_c\d+\.csv\.gz"
fat_files = sorted(re.findall(fat_pattern, html))
fatalities_file = fat_files[-1]
```

---

---

## Smoke-Test Bugs Found and Fixed (2026-05-08)

All 4 new pipelines ran correctly after 3 fixes to `run_real_case_study.py`:

| Bug | Location | Root cause | Fix |
|-----|----------|-----------|-----|
| `DRUNK_DR` column missing | `_load_nhtsa_sources`, `_NHTSA_SQL_VALID`, null_explosion fault | NHTSA FARS 2022 CSV does not have a `DRUNK_DR` column (present in some older/newer years) | Replaced with `PEDS` (pedestrians involved) throughout |
| `State Code` cast fails with `'CC'` | `_load_epa_sources` site_lookup, `_EPA_SQL_VALID` | `aqs_sites.csv` includes non-US entries with alphabetic state codes (e.g. Canada='CC') | Changed `cast(... as integer)` → `try_cast(... as integer)` and added `try_cast(...) is not null` filter |
| `FATALITY_AGE ~ '^[0-9]+$'` regex on BIGINT | `_NOAA_SQL_FATALITY_ENRICHED` | DuckDB inferred `FATALITY_AGE` as BIGINT from CSV; `~` operator requires VARCHAR | Replaced with `avg(try_cast(FATALITY_AGE as double))` |

Smoke test result (--no-lrllm --per-fault 2): **96/96 incidents generated, 0 errors** across all 8 pipelines.

---

## Full Run Log

Full LR-LLM run (720 incidents) started: 2026-05-08. Log at:
`lineagerank_workspace/suite_full_run.log`

*Last updated: 2026-05-08. Steps 1–7 complete. Step 8 (paper draft update) pending full run results.*
