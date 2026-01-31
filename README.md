# FoodDB Setup

This project connects to a CockroachDB cluster from a Python application.

## Importing USDA FoodData Central (Full CSV download)

The USDA CSV bundle lives in `usda/FoodData_Central_csv_2025-12-18` (not committed).

1. Install the importer dependency:

```bash
.venv/bin/pip install -r requirements.txt
```

2. Ensure `.env` contains `DATABASE_URL` (see below), then run the importer:

```bash
.venv/bin/python scripts/import_usda_fdc.py --schema usda | tee scratch/import_usda.log
```

To import only the tables we’ve prioritized so far (and log structured progress):

```bash
set -a; source .env; set +a; .venv/bin/python scripts/import_usda_fdc.py \
  --schema usda \
  --only food_category \
  --only food \
  --skip-indexes \
  --log-file scratch/import_food.jsonl \
  --log-format jsonl \
  --progress-every-s 10 \
  | tee scratch/import_food.out
```

Branded foods import:

```bash
set -a; source .env; set +a; .venv/bin/python scripts/import_usda_fdc.py \
  --schema usda \
  --only branded_food \
  --skip-indexes \
  --log-file scratch/import_branded.jsonl \
  --log-format jsonl \
  --progress-every-s 10 \
  | tee scratch/import_branded.out
```

Benchmark/estimate (loads one medium table into a throwaway schema and extrapolates total time):

```bash
.venv/bin/python scripts/import_usda_fdc.py --schema usda_bench --drop-schema --only food_update_log_entry --benchmark
```

If an import is interrupted, rerun per-table with `--only`/`--skip` and `--truncate` for any table that may be partial.
If an import fails mid-table and you want to continue without restarting, use `--resume` (it skips already-loaded rows based on the current table row count).

Resume example (append output/log):

```bash
set -a; source .env; set +a; .venv/bin/python scripts/import_usda_fdc.py \
  --schema usda \
  --only branded_food \
  --resume \
  --skip-indexes \
  --log-file scratch/import_branded.jsonl \
  --log-format jsonl \
  --progress-every-s 10 \
  | tee -a scratch/import_branded.out
```

Observability:
- `tail -f scratch/import_branded.out`
- `tail -f scratch/import_branded.jsonl`

Notes:
- The importer reads CSVs as **UTF-8 with `errors="strict"`** and loads via `psycopg` + `COPY FROM STDIN` (so values like `M&M's` are handled safely).
- The importer uses `psycopg` + `COPY FROM STDIN` for speed and to handle schema drift via CSV headers.
- CockroachDB has transaction lock limits; the importer loads in chunks (see `--chunk-rows`).
- In the `2025-12-18` USDA bundle, `food.csv`'s `food_category_id` column contains a mix of numeric IDs and branded category names (strings), so it is stored as `STRING`.
- The importer uses `usda/Download_Field_Descriptions_Oct2020.pdf` for coverage warnings, but the 2025 CSV release may contain newer columns/tables.
- Use `--skip-indexes` during initial loads to save RUs; add indexes after you’re confident the data fits.

## Indexes (recommended)

We typically import with `--skip-indexes` and then create indexes as a separate step so RU/storage impact is explicit.

Create the minimal recommended indexes (safe to re-run). This is the set we created for the current cluster (see `NOTES.md` “2026-01-31: Resume Notes”):

```bash
set -a; source .env; set +a; .venv/bin/python scripts/create_usda_indexes.py \
  --schema usda \
  --only food_fdc_id_idx \
  --only branded_food_gtin_upc_idx \
  --only food_nutrient_fdc_id_idx \
  --only food_portion_fdc_id_idx \
  --log-file scratch/indexes_usda.jsonl \
  --log-format jsonl \
  | tee scratch/indexes_usda.out
```

Minimal recommended set:
- `usda.branded_food(gtin_upc)` (barcode lookup)
- `usda.food(fdc_id)` (fast joins/lookups by FDC ID)
- `usda.food_nutrient(fdc_id)` (fetch nutrients for a food)
- `usda.food_portion(fdc_id)` (fetch portions for a food)

Optional indexes (create explicitly if/when needed; they were not created in the current cluster):
- `usda.food(description)` (`food_description_idx`; useful for substring search / browsing)
- `usda.survey_fndds_food(food_code)` (`survey_fndds_food_food_code_idx`; useful for FNDDS lookups)
- `usda.survey_fndds_food(wweia_category_code)` (`survey_fndds_food_wweia_category_code_idx`; useful for FNDDS category browsing)

Enforce `usda.food.fdc_id` as NOT NULL + UNIQUE (recommended for join correctness):

```bash
set -a; source .env; set +a; .venv/bin/python - <<'PY'
import os
import psycopg

with psycopg.connect(os.environ["DATABASE_URL"], connect_timeout=10) as conn:
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE usda.food ALTER COLUMN fdc_id SET NOT NULL")
        conn.commit()
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS food_fdc_id_uidx ON usda.food (fdc_id)")
        conn.commit()
print("ok")
PY
```

If you want to create them one-at-a-time (useful for RU monitoring), list available index names:

```bash
.venv/bin/python scripts/create_usda_indexes.py --schema usda --list
```

## Schema snapshot (recommended)

The importer follows CSV headers (USDA can drift). For a concrete reference of what’s currently in CockroachDB, we keep a generated DDL snapshot:

- `docs/schema/usda_2025-12-18.sql`

Regenerate it:

```bash
set -a; source .env; set +a; .venv/bin/python scripts/dump_cockroach_schema.py \
  --schema usda \
  --output docs/schema/usda_2025-12-18.sql
```

## What we’ve successfully imported (observed)

As of 2026-01-11, in schema `usda`:
- `food_category`, `food`, `branded_food`
- `nutrient`, `food_nutrient` (core nutrient amounts)
- `measure_unit`, `food_portion` (portion UX)
- FNDDS tables: `survey_fndds_food`, `wweia_food_category`, `input_food`, `fndds_ingredient_nutrient_value`, `fndds_derivation`, `retention_factor`
- Conversion/provenance tables: `food_nutrient_conversion_factor`, `food_calorie_conversion_factor`, `food_protein_conversion_factor`, `food_nutrient_derivation`, `food_nutrient_source`
- Small extras: `foundation_food`, `sr_legacy_food`, `food_component`, `microbe`

Observed Cockroach Cloud usage after the initial `food_category + food + branded_food` phase:
- Request Units: ~25.31M / 50M
- Logical storage used: ~1.58 GiB / 10 GiB

Observed Cockroach Cloud usage after importing all non-ignore tables:
- Request Units: ~112.44M / 150M
- Logical storage used: ~3.53 GiB / 10 GiB

## Prerequisites

- Python 3.x
- CockroachDB Basic or Standard cluster
- Cluster credentials (username and password)

## Initial Setup

### 1. Download CA Certificate

To connect to CockroachDB with proper server certificate verification, you need to download the CA certificate:

```bash
curl --create-dirs -o $HOME/.postgresql/root.crt 'https://cockroachlabs.cloud/clusters/0567a564-16c4-48a4-bb4c-435ee1bae1b1/cert'
```

This will create the certificate file at `~/.postgresql/root.crt`.

### 2. Configure Environment Variables

Create a `.env` file in the project root with your CockroachDB credentials:

```
DATABASE_URL="postgresql://<username>:<password>@<host>:26257/<database>?sslmode=verify-full"
```

Replace placeholders with your actual CockroachDB cluster credentials.

## Replicating on Another Machine

To set up this project on a new machine:

1. Clone the repository
2. Download the CA certificate using the curl command above
3. Create a `.env` file with your CockroachDB credentials
4. Install required Python dependencies (see requirements.txt if available)
5. Run your application

## Notes

- The CA certificate must be located at `~/.postgresql/root.crt` for the connection to work
- Keep your `.env` file secure and never commit it to version control (it should be in `.gitignore`)
