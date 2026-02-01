# OpenFoodFacts (OFF) ingestion

This repo can ingest the OpenFoodFacts product export (tab-separated text in a `.csv.gz` file) into
CockroachDB under schema `openfoodfacts`.

## Download

Download (and keep compressed):

- `data/openfoodfacts/en.openfoodfacts.org.products.csv.gz`

The `data/` directory is gitignored.

## Import

Preflight (required; detects duplicate codes and writes a manifest with SHA-256):

```bash
set -a; source .env; set +a
python -m scripts.preflight_openfoodfacts \
  --tsv-path data/openfoodfacts/en.openfoodfacts.org.products.csv.gz \
  --manifest-out scratch/off_preflight.json \
  --duplicate-codes-out scratch/off_duplicate_codes.txt \
  --field-size-limit 20000000 \
  --log-format jsonl \
  --log-file scratch/preflight_openfoodfacts.jsonl
```

Recommended first run (truncate + minimal nutrients; skip secondary indexes during ingest):

```bash
set -a; source .env; set +a
python -m scripts.import_openfoodfacts \
  --tsv-path data/openfoodfacts/en.openfoodfacts.org.products.csv.gz \
  --schema openfoodfacts \
  --truncate \
  --nutrients minimal \
  --preflight-manifest scratch/off_preflight.json \
  --duplicate-codes scratch/off_duplicate_codes.txt \
  --skip-indexes \
  --field-size-limit 20000000 \
  --log-format jsonl \
  --log-file scratch/import_openfoodfacts.jsonl
```

Smoke test on a small subset:

```bash
set -a; source .env; set +a
python -m scripts.import_openfoodfacts \
  --tsv-path data/openfoodfacts/en.openfoodfacts.org.products.csv.gz \
  --schema openfoodfacts \
  --truncate \
  --nutrients minimal \
  --preflight-manifest scratch/off_preflight.json \
  --duplicate-codes scratch/off_duplicate_codes.txt \
  --field-size-limit 20000000 \
  --max-rows 5000
```

Notes:
- The importer defaults to delimiter tab (`\t`) and logs the chosen dialect. OFF exports are often
  tab-separated even when named `.csv`.
- v1 does not support `--resume`; use `--truncate` for repeat runs.
- The importer retries transient Cockroach `SERIALIZATION_FAILURE` errors (configurable via
  `--retries` and `--retry-sleep-s`).
- `code_norm` is stored as a string and preserves leading zeros; rows that normalize to an empty code
  are skipped.
- The importer refuses to run unless `--preflight-manifest` or `--expected-sha256` is provided.
- `--skip-indexes` skips secondary index creation only; the primary key index is always maintained
  (currently there are no secondary OFF indexes configured).
- Use `--encoding-errors` or `--field-size-limit` to handle invalid UTF-8 or very large fields.
- Preflight uses the system `sort` command and a temporary file on disk; use `--sort-tmp-dir` if needed.
- If preflight reports duplicates, pass `--duplicate-codes` so the importer can resolve them deterministically.
- Preflight exits with code 0 even if duplicates are found; rely on the manifest/logs for that signal.

## Tables

Schema: `openfoodfacts`

- `product_raw`: selected identity fields + a small set of nutriments (columns are normalized for SQL).
- `nutrient_100g`: EAV table of extracted `_100g` nutrient values.
- `import_metadata`: per-import metadata for reproducibility / attribution (source URL, file hash, etc.).

## Nutrients

Default `--nutrients minimal` extracts:

- energy (`energy-kcal_100g`, `energy-kj_100g`, and fallback `energy_100g` as kJ)
- `fat_100g`
- `saturated-fat_100g`
- `carbohydrates_100g`
- `sugars_100g`
- `fiber_100g`
- `proteins_100g`
- `sodium_100g`
- optional `salt_100g` via `--include-salt`

The `--nutrients all` mode extracts all `*_100g` fields into `nutrient_100g` (explicit RU/storage
decision).

## License / ODbL

OpenFoodFacts data is provided under ODbL; if you redistribute derived databases, capture attribution
and share-alike requirements. This repo records import provenance in `openfoodfacts.import_metadata`
to help with compliance.
