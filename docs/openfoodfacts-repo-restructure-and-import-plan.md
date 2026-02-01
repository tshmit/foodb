# Repo Restructuring + OpenFoodFacts Import: Plan

## 0) Goals (near-term)

- Keep existing USDA tooling working unchanged while introducing structure for multi-source ingestion.
- Add OpenFoodFacts (OFF) ingestion from OFF CSV/TSV export into a dedicated Cockroach schema.
- Establish conventions that make later overlap analysis straightforward (barcode normalization, nutrient
  extraction).
- Make “expensive ingest choices” explicit (e.g. nutrient scope).

---

## 1) Repository layout + conventions

### 1.1 Top-level directories

- `foodb/` (new, committed Python package for shared code + source modules)
- `scripts/` (thin CLIs / wrappers; should be importable as a module package)
- `docs/` (committed docs and schema snapshots)
- `data/` (new, gitignored raw dumps: OFF/USDA/etc)
- `scratch/` (already exists; keep gitignored logs/output)

### 1.2 Python entrypoint convention (import path)

- Prefer module entrypoints: run scripts via `python -m ...`
  - Example: `python -m scripts.import_openfoodfacts ...`
- Make `scripts/` importable by adding `scripts/__init__.py`
- Do **not** require packaging or bootstrap hacks yet (no `pip install -e .` requirement for this step).
- Keep existing “direct script” invocations working (especially USDA) during transition.

### 1.3 Database convention

- One Cockroach database.
- One schema per source:
  - `usda`
  - `openfoodfacts`
- (Optional later) `core` schema for cross-source mapping, not part of this step.

---

## 2) Git hygiene: committed code/docs vs raw data

- Create `data/` and add it to `.gitignore` (e.g. `data/`).
- OFF raw exports live under `data/openfoodfacts/...` (gitignored).
- **Do not move USDA dumps yet**:
  - Keep `usda/FoodData_Central_csv_2025-12-18` working as the USDA importer default.
  - Update docs to *recommend* `data/usda/...` as a preferred location for new setups.
  - Later (not now) update USDA importer to accept both locations (keep current default; allow
    `--csv-dir data/usda/...`).

---

## 3) Package structure (new `foodb/`)

Start small; do not migrate USDA code unless required by the import-path convention.

### 3.1 Shared DB utilities

- `foodb/db/`
  - `connect.py` (reads `DATABASE_URL`, opens psycopg connection, common connect options)
  - `ident.py` (identifier normalization + quoting helpers)
  - `copy.py` (COPY helpers / chunking patterns as needed)
  - `logging.py` (JSONL/text event logging compatible with existing script patterns)

### 3.2 Shared normalization

- `foodb/normalize/`
  - `barcode.py`
    - normalize `code_raw` → `code_norm` as **string**
    - strip non-digits only; **never convert to int**; **never drop leading zeros**
    - (optional) return basic metadata like length/validity flags for logging

### 3.3 Source modules

- `foodb/sources/openfoodfacts/`
  - `schema.py` (DDL for OFF tables)
  - `ingest_tsv.py` (TSV parsing + load; supports `.gz` input)
  - `nutrients.py` (nutrient subset definition + mapping from OFF header → nutrient_key/unit)
  - `indexes.py` (index specs; minimal recommended set)

(USDA module migration is explicitly deferred.)

---

## 4) Scripts as thin entrypoints (backwards compatible)

### 4.1 USDA (no breakage)

- Keep `scripts/import_usda_fdc.py` working as-is (including its default `--csv-dir usda/...`).
- Keep `scripts/create_usda_indexes.py` as-is.
- Keep `scripts/dump_cockroach_schema.py` as-is.

### 4.2 Add OFF entrypoint

- Add `scripts/import_openfoodfacts.py` as a thin wrapper that calls
  `foodb.sources.openfoodfacts...`.
- Add `scripts/__init__.py` so the preferred usage is:
  - `python -m scripts.import_openfoodfacts ...`

---

## 5) Docs + schema snapshots

- Add `docs/sources/openfoodfacts.md` covering:
  - where to download OFF CSV/TSV export
  - expected filename conventions / where to place it under `data/openfoodfacts/...`
  - licensing/ODbL notes (brief and actionable)
  - what we ingest vs ignore (at least for “minimal nutrients”)
  - known quirks (TSV, encoding, huge size, column naming)

- Schema snapshots:
  - keep existing `docs/schema/usda_2025-12-18.sql`
  - add OFF snapshot after first successful import:
    - minimal change: `docs/schema/openfoodfacts_<date>.sql`
    - (optional later) move to `docs/schema/openfoodfacts/<date>.sql`

---

## 6) OFF import design (TSV-first)

### 6.1 Input

- OFF export placed under `data/openfoodfacts/...` (gitignored).
- Support both plain and gzipped inputs:
  - `--tsv-path data/openfoodfacts/...products.csv.gz` (or `.tsv.gz`), read as UTF‑8
- CSV vs TSV dialect must be explicit:
  - Accept `--delimiter` (default: tab).
  - Also auto-detect delimiter from the header line (e.g. “many tabs” vs “many commas”) and log the
    chosen delimiter.
- CLI args:
  - `--tsv-path` (required)
  - `--schema openfoodfacts` (default)
  - `--truncate` (optional; recommended for v1)
  - `--log-file scratch/import_openfoodfacts.jsonl`
  - `--nutrients minimal|all` (default: `minimal`)

### 6.2 Identifier normalization (important)

- Normalize OFF column names for SQL friendliness:
  - e.g. `saturated-fat_100g` → `saturated_fat_100g`
- Maintain an explicit mapping (OFF header → SQL column name) so behavior is deterministic and
  debuggable.

### 6.3 Tables (schema `openfoodfacts`)

Start with two tables optimized for near-term nutrition + barcode use.

1) `openfoodfacts.product_raw`

- Purpose: stable “identity + selected fields” per product.
- Uniqueness policy (v1):
  - `code_norm` is `NOT NULL` and is the `PRIMARY KEY`.
  - Skip rows where `code_norm` normalization yields empty.
  - Keep `code_raw` as a non-key field for provenance/debugging.
- Columns (initial set; names normalized):
  - `code_raw STRING` (as in OFF)
  - `code_norm STRING` (digits-only string; preserves leading zeros)
  - `product_name STRING` (and optionally `product_name_en` if present)
  - `brands STRING`
  - `categories STRING`
  - `quantity STRING`
  - `serving_size STRING`
  - `last_modified_t INT8` (if present)
  - selected nutriment columns you care about (see minimal subset below)

2) `openfoodfacts.nutrient_100g`

- Purpose: normalized nutrient representation for comparisons and future joins.
- Columns:
  - `code_norm STRING`
  - `nutrient_key STRING` (e.g. `energy_kcal`, `fat`, `saturated_fat`, `carbohydrates`, `sugars`,
    `fiber`, `protein`, `sodium`, `salt`)
  - `value FLOAT8`
  - `unit STRING`
  - `source_field STRING` (original OFF field name)

3) `openfoodfacts.import_metadata` (ODbL compliance + reproducibility)

- Purpose: capture dataset version/date and attribution metadata alongside the import.
- Columns (initial set):
  - import timestamp
  - source URL(s)
  - local filename/path
  - dataset “as of” date if available
  - file hash (e.g. SHA-256)

### 6.4 Nutrient scope decision (explicit cost control)

- Default `--nutrients minimal` ingests only an opinionated subset into `nutrient_100g`:
  - energy: `energy-kcal_100g` (and/or `energy-kj_100g`)
  - `fat_100g`
  - `saturated-fat_100g`
  - `carbohydrates_100g`
  - `sugars_100g`
  - `fiber_100g`
  - `proteins_100g`
  - `sodium_100g`
  - optionally `salt_100g`
- `--nutrients all` extracts all `*_100g` nutriment columns to `nutrient_100g` (explicit RU/storage
  decision).

### 6.5 Indexes (minimal)

- `openfoodfacts.product_raw(code_norm)`
- `openfoodfacts.nutrient_100g(code_norm)`
- Optional later: `openfoodfacts.nutrient_100g(nutrient_key)` if query patterns need it.

### 6.6 Ingest steps

- Read header row, build header→normalized-column mapping.
- Stream rows (support `.gz`), parse minimal selected fields.
- Compute `code_norm` via shared barcode normalizer.
- Insert into `product_raw` (bulk/COPY style if possible).
- Extract nutrient subset (or all) into `nutrient_100g`.
- Emit structured logs (counts, timing, basic anomalies).
- Capture `import_metadata` (timestamp, source, filename, hash).

### 6.7 Verification (first-pass)

- Row counts:
  - products ingested
  - nutrient rows ingested
- Integrity shape checks:
  - count/% with missing `code_raw`
  - count/% with empty `code_norm` after normalization
  - top missing nutrients by product count (for minimal subset)

---

## 7) Milestones (incremental delivery)

1) Add `data/` + `.gitignore` update; add `scripts/__init__.py`; add `foodb/` skeleton with shared DB
   + barcode helpers.
2) Add OFF module (`foodb/sources/openfoodfacts/...`) + `python -m scripts.import_openfoodfacts`
   entrypoint; support gz input.
3) Add OFF docs page + generate first OFF schema snapshot after import.
4) (Optional later) USDA migration into `foodb/sources/usda/` + add module entrypoints, without
   breaking existing direct scripts.

---

## Deferred (explicitly not in v1)

- `--resume`:
  - v1: no resume; use `--truncate`.
  - v2: implement idempotent ingest via upsert by key (`code_norm`) for `product_raw`, and rebuild
    `nutrient_100g` deterministically (delete+insert per `code_norm` or truncate per run).
- JSON/JSONB payload columns:
  - v1: omit JSON column (TSV-first; avoid implying a canonical raw record is preserved).
  - v2: if needed, add `selected_fields_json` using `JSONB` explicitly for selected fields, or a
    stringified raw row representation if portability is preferred.
