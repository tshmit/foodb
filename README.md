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

As of 2026-01-10, in schema `usda`:
- `food_category`: 28 rows
- `food`: 2,085,340 rows
- `branded_food`: 1,993,975 rows

Observed Cockroach Cloud usage after `food_category + food + branded_food`:
- Request Units: ~25.31M / 50M
- Logical storage used: ~1.58 GiB / 10 GiB

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
