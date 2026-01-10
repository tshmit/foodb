# USDA FoodData Central import notes (CockroachDB)

This repo uses `scripts/import_usda_fdc.py` to import the USDA “Full Download” CSVs into CockroachDB (connection string from `.env` `DATABASE_URL`).

## Source-of-truth for schema

- `usda/Download_Field_Descriptions_Oct2020.pdf` is useful for semantics, but it is **out of sync** with the 2025 CSV bundle (USDA adds/changes columns without promptly updating the PDF).
- The importer treats the **CSV headers as the schema source-of-truth** and logs coverage gaps as `dictionary_miss_headers` when the PDF doesn’t mention columns that exist in the CSV.

## CSV quirks discovered in `2025-12-18`

- `food.csv` `food_category_id` is not reliably numeric; it includes branded category strings like `"Oils Edible"`. We store it as `STRING` to avoid type errors.
- `food.csv` `publication_date` appears in mixed formats (ISO `YYYY-MM-DD` and `M/D/YYYY`). The importer normalizes `M/D/YYYY` to ISO.
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

## What we imported and observed (as of 2026-01-10)

In schema `usda`:
- `food_category`: 28 rows
- `food`: 2,085,340 rows
- `branded_food`: 1,993,975 rows

Schema snapshot (generated from CockroachDB):
- `docs/schema/usda_2025-12-18.sql`

Observed Cockroach Cloud usage after `food_category + food + branded_food`:
- Request Units: ~25.31M / 50M
- Logical storage used: ~1.58 GiB / 10 GiB

## Fast completeness checks

Row counts vs CSV:
- `wc -l usda/FoodData_Central_csv_2025-12-18/food.csv` (minus header) should match `SELECT count(*) FROM usda.food`.
- `wc -l usda/FoodData_Central_csv_2025-12-18/branded_food.csv` (minus header) should match `SELECT count(*) FROM usda.branded_food`.

Sanity checks:

```sql
SELECT count(*) FROM usda.food;
SELECT count(*) FROM usda.food_category;
SELECT count(*) FROM usda.branded_food;
SELECT count(*) FROM usda.branded_food WHERE fdc_id IS NULL;
SELECT count(*) FROM usda.branded_food bf
LEFT JOIN usda.food f ON f.fdc_id = bf.fdc_id
WHERE f.fdc_id IS NULL;
```
