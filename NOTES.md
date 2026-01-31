# USDA FoodData Central import notes (CockroachDB)

This repo uses `scripts/import_usda_fdc.py` to import the USDA “Full Download” CSVs into CockroachDB (connection string from `.env` `DATABASE_URL`).

## 2026-01-31: Resume Notes

After a couple weeks away, I re-verified both the codebase and the live CockroachDB state so we can commit with confidence.

### What was confirmed in the live DB

- Connectivity: able to connect using `.env` `DATABASE_URL` and run a trivial `SELECT 1`.
- Database/schema: connected DB is `defaultdb`, and the imported schema is `usda`.
- Tables present: `usda` contains 22 tables including the “core” tables (`food`, `branded_food`, `nutrient`, `food_nutrient`, `measure_unit`, `food_portion`) plus dataset extensions / FNDDS / conversion / provenance tables (matching the scope described below).
- Key row counts (`SELECT count(*)`):
  - `usda.food`: 2,085,340
  - `usda.branded_food`: 1,993,975
  - `usda.food_nutrient`: 27,094,028 (the large import is complete)
  - `usda.food_portion`: 47,446
  - `usda.nutrient`: 477
  - `usda.measure_unit`: 123

### Integrity checks performed (and the one known anomaly)

I ran “missing join” checks to detect partial imports or dangling references:

- `food_nutrient → food` missing joins: 0
- `food_nutrient → nutrient` missing joins: 0
- `food_portion → food` missing joins: 0
- `food_portion → measure_unit` missing joins: 0

The only notable data quirk matches what we previously observed:

- `food_portion` contains 273 rows with `fdc_id IS NULL` and `measure_unit_id IS NULL`.
  - These do not break joins (because they cannot join to anything), but they will show up in naive integrity queries unless filtered.
  - Recommended handling for app queries: filter portions with `WHERE fdc_id IS NOT NULL`.

### Indexes/constraints verified

From `SHOW CREATE TABLE` / `SHOW INDEXES`, the DB currently has:

- `usda.food`:
  - `fdc_id` is `NOT NULL`
  - `food_fdc_id_uidx` (unique) and `food_fdc_id_idx`
- `usda.branded_food`:
  - `branded_food_gtin_upc_idx` (barcode lookups)
- `usda.food_nutrient`:
  - `food_nutrient_fdc_id_idx`
- `usda.food_portion`:
  - `food_portion_fdc_id_idx`

I also spot-checked that CockroachDB uses `branded_food_gtin_upc_idx` for `WHERE gtin_upc = ...` via `EXPLAIN`.

### Schema snapshot consistency

`docs/schema/usda_2025-12-18.sql` matches a fresh dump from the current DB using `scripts/dump_cockroach_schema.py`, so the repo snapshot reflects the DB we’re working with.

### Code health / “tested?”

- There is no obvious test suite in this repo (no `tests/` found), and `pytest` is not installed in the venv.
- `ruff check .` passes, and `ruff format --check .` is clean.

### Index defaults discrepancy + hypothesis (documenting what we know)

While resuming, we noticed a mismatch between documentation and the current uncommitted index script:

- `README.md` describes a minimal “recommended” index set of four: `food(fdc_id)`, `branded_food(gtin_upc)`, `food_nutrient(fdc_id)`, `food_portion(fdc_id)`.
- `scripts/create_usda_indexes.py` currently defines a larger default set (adds `food_description_idx` and two `survey_fndds_food_*` indexes).

What we can confirm today:

- The live DB does not have `food_description_idx` or the two `survey_fndds_food_*` indexes.
- The live DB does have the minimal “core” indexes above, and the `scratch/indexes_minimal.jsonl` logs show indexes were created in deliberately small runs.

Hypothesis about how we got here:

- We likely ran `scripts/create_usda_indexes.py` with `--only ...` / `--skip ...` (or equivalent) to intentionally keep the index footprint small for RU/storage reasons.
- Alternatively, the script’s defaults may have been smaller at the time, and the extra defaults were added later during local iteration (the script is currently uncommitted, so we can’t confirm history yet).

### Timeline summary (from `scratch/*.jsonl`, UTC)

The `scratch/` JSONL logs capture event timestamps (`run_start`, `run_resume`, `schema_drop`, `table_truncate`, index events), which is enough to reconstruct the high-level sequence:

- 2026-01-10: benchmark run; destructive schema initialization via `--drop-schema`; initial `food` load hit a `publication_date` parse error; `food` later completed via `--resume`; `branded_food` imported.
- 2026-01-11: `nutrient` imported; `food_nutrient` imported with one interruption and then completed via `--resume`; portions/extensions loaded; “remaining tables” run executed with per-table truncation (`--truncate`); indexes created in small, selected batches.

## Source-of-truth for schema

- `usda/Download_Field_Descriptions_Oct2020.pdf` is useful for semantics, but it is **out of sync** with the 2025 CSV bundle (USDA adds/changes columns without promptly updating the PDF).
- The importer treats the **CSV headers as the schema source-of-truth** and logs coverage gaps as `dictionary_miss_headers` when the PDF doesn’t mention columns that exist in the CSV.

## CSV quirks discovered in `2025-12-18`

- `food.csv` `food_category_id` is not reliably numeric; it includes branded category strings like `"Oils Edible"`. We store it as `STRING` to avoid type errors.
- `food.csv` `publication_date` appears in mixed formats (ISO `YYYY-MM-DD` and `M/D/YYYY`). The importer normalizes `M/D/YYYY` to ISO.
- `food_portion.csv` includes 273 rows with an `id` but missing `fdc_id` and `measure_unit_id`; these show up as “missing joins” in integrity checks. They can be ignored by filtering `WHERE fdc_id IS NOT NULL`.
- `all_downloaded_table_record_counts.csv` can be stale/wrong (at least for `food.csv`). For resume/verification, compare against CSV line counts.

## Importer behavior (important operational details)

- Uses `psycopg` v3 with `COPY FROM STDIN` for throughput and RU efficiency.
- Reads CSV as UTF-8 with `errors="strict"` (the “M&M's” case is fine; this is about invalid byte sequences).
- Commits in chunks (`--chunk-rows`, default `200000`) to avoid Cockroach transaction lock-intent budget issues.
- `--skip-indexes` defers post-import index creation (useful while validating RU/storage fit).
- `--resume` continues mid-table by skipping `count(*)` already committed rows. This is only safe because we commit after each chunk.
  - Don’t use `--resume` with `--drop-schema` or `--truncate`.
  - Prefer `--retries 0` when resuming to avoid retry-side effects.
- Apparent “stalls” for several minutes can happen during a chunk flush/commit (no new progress events until the commit completes).

## What we imported and observed (as of 2026-01-11)

In schema `usda` (excluding “Ignore” tables from `FDC-tables-assessment.md`):
- Core: `food_category`, `food`, `branded_food`, `nutrient`, `food_nutrient`
- Portions: `measure_unit`, `food_portion`
- Dataset extensions: `foundation_food`, `sr_legacy_food`, `survey_fndds_food`, `wweia_food_category`
- FNDDS composition: `input_food`, `fndds_ingredient_nutrient_value`, `fndds_derivation`, `retention_factor`
- Conversion/provenance: `food_nutrient_conversion_factor`, `food_calorie_conversion_factor`, `food_protein_conversion_factor`, `food_nutrient_derivation`, `food_nutrient_source`
- Extras: `food_component`, `microbe`

Schema snapshot (generated from CockroachDB):
- `docs/schema/usda_2025-12-18.sql`

Observed Cockroach Cloud usage after the initial `food_category + food + branded_food` phase:
- Request Units: ~25.31M / 50M
- Logical storage used: ~1.58 GiB / 10 GiB

Observed Cockroach Cloud usage after importing all non-ignore tables:
- Request Units: ~112.44M / 150M
- Logical storage used: ~3.53 GiB / 10 GiB

## Fast completeness checks

Row counts vs CSV:
- `wc -l usda/FoodData_Central_csv_2025-12-18/food.csv` (minus header) should match `SELECT count(*) FROM usda.food`.
- `wc -l usda/FoodData_Central_csv_2025-12-18/branded_food.csv` (minus header) should match `SELECT count(*) FROM usda.branded_food`.
- `wc -l usda/FoodData_Central_csv_2025-12-18/food_nutrient.csv` (minus header) should match `SELECT count(*) FROM usda.food_nutrient`.
- `wc -l usda/FoodData_Central_csv_2025-12-18/nutrient.csv` (minus header) should match `SELECT count(*) FROM usda.nutrient`.
- `wc -l usda/FoodData_Central_csv_2025-12-18/food_portion.csv` (minus header) should match `SELECT count(*) FROM usda.food_portion`.
- `wc -l usda/FoodData_Central_csv_2025-12-18/measure_unit.csv` (minus header) should match `SELECT count(*) FROM usda.measure_unit`.

Sanity checks:

```sql
SELECT count(*) FROM usda.food;
SELECT count(*) FROM usda.food_category;
SELECT count(*) FROM usda.branded_food;
SELECT count(*) FROM usda.nutrient;
SELECT count(*) FROM usda.food_nutrient;
SELECT count(*) FROM usda.measure_unit;
SELECT count(*) FROM usda.food_portion;

SELECT count(*) FROM usda.branded_food WHERE fdc_id IS NULL;
SELECT count(*) FROM usda.branded_food bf
LEFT JOIN usda.food f ON f.fdc_id = bf.fdc_id
WHERE f.fdc_id IS NULL;

-- These should be 0 (or extremely close to 0 if USDA has dangling references).
SELECT count(*) FROM usda.food_nutrient fn
LEFT JOIN usda.food f ON f.fdc_id = fn.fdc_id
WHERE f.fdc_id IS NULL;
SELECT count(*) FROM usda.food_nutrient fn
LEFT JOIN usda.nutrient n ON n.id = fn.nutrient_id
WHERE n.id IS NULL;
SELECT count(*) FROM usda.food_portion fp
LEFT JOIN usda.food f ON f.fdc_id = fp.fdc_id
WHERE f.fdc_id IS NULL;
SELECT count(*) FROM usda.food_portion fp
LEFT JOIN usda.measure_unit mu ON mu.id = fp.measure_unit_id
WHERE mu.id IS NULL;

-- Index sanity:
SHOW INDEXES FROM usda.food;
SHOW INDEXES FROM usda.branded_food;
SHOW INDEXES FROM usda.food_nutrient;
```
