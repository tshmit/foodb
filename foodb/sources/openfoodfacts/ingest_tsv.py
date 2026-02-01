from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import random
import time
from pathlib import Path

import psycopg
from psycopg import sql

from foodb.db.connect import connect
from foodb.db.logging import Logger
from foodb.normalize.barcode import normalize_barcode
from foodb.sources.openfoodfacts.indexes import ddl as index_ddl
from foodb.sources.openfoodfacts.nutrients import (
    minimal_nutrients,
    normalize_nutrient_key_from_field,
    unit_for_source_field,
)
from foodb.sources.openfoodfacts.schema import ddl as schema_ddl


def _detect_delimiter(header_line: str) -> str:
    tabs = header_line.count("\t")
    commas = header_line.count(",")
    if commas > tabs and commas > 0:
        return ","
    return "\t"


def _float_or_empty(value: str) -> str:
    value = value.strip()
    if value == "":
        return ""
    try:
        float(value)
    except ValueError:
        return ""
    return value


def _int_or_empty(value: str) -> str:
    value = value.strip()
    if value == "":
        return ""
    if value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
        return value
    return ""


def _escape_copy_text(value: str) -> str:
    return (
        value.replace("\x00", "")
        .replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\b", "\\b")
        .replace("\f", "\\f")
        .replace("\v", "\\v")
    )


def _copy_cell(value: str, *, null_if_empty: bool) -> str:
    if null_if_empty and value == "":
        return "\\N"
    return _escape_copy_text(value)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_preflight_manifest(path: Path) -> dict[str, object]:
    try:
        with path.open("r", encoding="utf-8", errors="strict") as f:
            data = json.load(f)
    except FileNotFoundError:
        raise SystemExit(f"Missing preflight manifest: {path}") from None
    except json.JSONDecodeError as e:
        raise SystemExit(f"Invalid preflight manifest JSON: {path} ({e})") from None
    if not isinstance(data, dict):
        raise SystemExit(f"Invalid preflight manifest (expected JSON object): {path}")
    return data


def _manifest_int(data: dict[str, object], key: str) -> int | None:
    if key not in data:
        return None
    try:
        return int(data[key])  # type: ignore[arg-type]
    except (TypeError, ValueError):
        raise SystemExit(f"Invalid preflight manifest field {key!r} (expected int)") from None


def _manifest_bool(data: dict[str, object], key: str) -> bool | None:
    if key not in data:
        return None
    value = data[key]
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lower = value.strip().lower()
        if lower in {"true", "false"}:
            return lower == "true"
    raise SystemExit(f"Invalid preflight manifest field {key!r} (expected bool)")


def _load_duplicate_codes(path: Path) -> set[str]:
    try:
        with path.open("r", encoding="utf-8", errors="strict") as f:
            codes = {line.strip() for line in f if line.strip()}
    except FileNotFoundError:
        raise SystemExit(f"Missing duplicate codes file: {path}") from None
    return codes


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import OpenFoodFacts TSV/CSV export into CockroachDB."
    )
    parser.add_argument(
        "--tsv-path",
        type=Path,
        required=True,
        help="Path to OFF export (.csv/.tsv, optionally .gz).",
    )
    parser.add_argument("--schema", default="openfoodfacts", help="Target schema name.")
    parser.add_argument(
        "--truncate", action="store_true", help="Truncate OFF tables before import (recommended)."
    )
    parser.add_argument("--delimiter", default="\t", help="Field delimiter (default: tab).")
    parser.add_argument(
        "--nutrients",
        choices=["minimal", "all"],
        default="minimal",
        help="Which nutrient fields to extract into nutrient_100g.",
    )
    parser.add_argument(
        "--include-salt",
        action="store_true",
        help="Include salt_100g in minimal nutrient extraction.",
    )
    parser.add_argument("--chunk-rows", type=int, default=20_000, help="Rows per COPY transaction.")
    parser.add_argument(
        "--max-rows", type=int, default=0, help="Optional cap for testing (0 means no cap)."
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=10,
        help="Retry count for transient Cockroach transaction errors (e.g. serialization failures).",
    )
    parser.add_argument(
        "--retry-sleep-s",
        type=float,
        default=0.5,
        help="Base sleep between retries (seconds). Uses exponential backoff + small jitter.",
    )
    parser.add_argument(
        "--expected-sha256",
        default=None,
        help="Require the input file SHA-256 to match this value (from preflight).",
    )
    parser.add_argument(
        "--preflight-manifest",
        type=Path,
        default=None,
        help="Path to preflight manifest JSON; importer refuses to run if it doesn't match.",
    )
    parser.add_argument(
        "--duplicate-codes",
        type=Path,
        default=None,
        help="Path to duplicate code_norm list (one per line) from preflight.",
    )
    parser.add_argument(
        "--duplicate-policy",
        choices=["last_modified_completeness"],
        default="last_modified_completeness",
        help="Policy for choosing among duplicate code_norm rows.",
    )
    parser.add_argument(
        "--field-size-limit",
        type=int,
        default=2_000_000,
        help="Override csv.field_size_limit to avoid large-field parse errors.",
    )
    parser.add_argument(
        "--encoding-errors",
        choices=["strict", "replace", "surrogateescape"],
        default="strict",
        help="How to handle invalid UTF-8 sequences when reading the file.",
    )
    parser.add_argument(
        "--skip-indexes",
        action="store_true",
        help="Skip secondary index creation (primary key still enforced).",
    )
    parser.add_argument(
        "--dedupe",
        choices=["none", "memory"],
        default="none",
        help="Duplicate handling for code_norm: none (fast) or memory (skip duplicates).",
    )
    parser.add_argument("--log-file", type=Path, default=None, help="Optional JSONL/text log file.")
    parser.add_argument(
        "--log-format", choices=["text", "jsonl"], default="text", help="Log output format."
    )
    parser.add_argument(
        "--database-url-env", default="DATABASE_URL", help="Env var name containing DB URL."
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logger = Logger(fmt=args.log_format, log_file=args.log_file)
    try:
        if not args.tsv_path.exists():
            raise SystemExit(f"Missing input: {args.tsv_path}")

        try:
            csv.field_size_limit(args.field_size_limit)
        except OverflowError:
            raise SystemExit(f"--field-size-limit is too large: {args.field_size_limit}") from None

        if not args.expected_sha256 and not args.preflight_manifest:
            raise SystemExit(
                "Missing preflight identity: pass --expected-sha256 or --preflight-manifest"
            )

        file_sha256 = _sha256(args.tsv_path)
        file_bytes = args.tsv_path.stat().st_size

        if args.expected_sha256 and file_sha256 != args.expected_sha256:
            raise SystemExit(
                f"SHA-256 mismatch: expected {args.expected_sha256}, got {file_sha256}"
            )

        manifest_duplicates = False
        manifest_duplicate_codes_path: str | None = None
        if args.preflight_manifest:
            manifest = _load_preflight_manifest(args.preflight_manifest)
            manifest_sha = manifest.get("file_sha256")
            if not manifest_sha:
                raise SystemExit("Preflight manifest missing file_sha256")
            if manifest_sha != file_sha256:
                raise SystemExit(
                    f"Preflight SHA-256 mismatch: manifest {manifest_sha}, file {file_sha256}"
                )
            manifest_bytes = _manifest_int(manifest, "file_bytes")
            if manifest_bytes is None:
                raise SystemExit("Preflight manifest missing file_bytes")
            if manifest_bytes != file_bytes:
                raise SystemExit(
                    f"Preflight byte-size mismatch: manifest {manifest_bytes}, file {file_bytes}"
                )
            duplicates_found = _manifest_bool(manifest, "duplicates_found")
            if duplicates_found is None:
                raise SystemExit("Preflight manifest missing duplicates_found")
            manifest_duplicate_codes_path = manifest.get("duplicate_codes_path")  # type: ignore[assignment]
            if manifest_duplicate_codes_path is not None and not isinstance(
                manifest_duplicate_codes_path, str
            ):
                raise SystemExit("Preflight manifest duplicate_codes_path must be a string")
            duplicate_values = _manifest_int(manifest, "duplicate_values") or 0
            duplicate_occurrences = _manifest_int(manifest, "duplicate_occurrences") or 0
            manifest_duplicates = (
                duplicates_found or duplicate_values > 0 or duplicate_occurrences > 0
            )

        if args.dedupe == "memory" and args.duplicate_codes is not None:
            raise SystemExit("Use either --dedupe memory or --duplicate-codes, not both.")

        duplicate_codes_path: Path | None = args.duplicate_codes
        if duplicate_codes_path is None and manifest_duplicate_codes_path:
            duplicate_codes_path = Path(str(manifest_duplicate_codes_path))
            if not duplicate_codes_path.is_absolute():
                duplicate_codes_path = (
                    args.preflight_manifest.parent / duplicate_codes_path
                ).resolve()
        duplicate_codes = (
            _load_duplicate_codes(duplicate_codes_path) if duplicate_codes_path else None
        )

        if manifest_duplicates and args.dedupe == "none" and not duplicate_codes:
            raise SystemExit(
                "Preflight reported duplicates; pass --duplicate-codes from preflight (preferred) or use --dedupe memory."
            )

        open_fn = gzip.open if args.tsv_path.suffix == ".gz" else open
        with open_fn(
            args.tsv_path,
            "rt",
            encoding="utf-8",
            errors=args.encoding_errors,
            newline="",
        ) as f:
            header_line = f.readline()
            if not header_line:
                raise SystemExit(f"Empty file: {args.tsv_path}")

            if header_line.startswith("\ufeff"):
                header_line = header_line.lstrip("\ufeff")

            detected = _detect_delimiter(header_line)
            delimiter = args.delimiter
            if args.delimiter == "\t" and detected != "\t":
                delimiter = detected
            logger.event("dialect", delimiter=repr(delimiter), detected=repr(detected))

            headers = next(csv.reader([header_line], delimiter=delimiter))
            header_to_index = {h: i for i, h in enumerate(headers)}

            def get(row: list[str], name: str) -> str:
                idx = header_to_index.get(name)
                if idx is None or idx >= len(row):
                    return ""
                return row[idx]

            if "code" not in header_to_index:
                raise SystemExit("Input is missing required column: code")

            if args.nutrients == "minimal":
                specs = minimal_nutrients(include_salt=args.include_salt)
                by_field = {s.source_field: (s.nutrient_key, s.unit) for s in specs}
                nutrient_fields = [s.source_field for s in specs]
            else:
                by_field = {}
                nutrient_fields = [h for h in headers if h.endswith("_100g")]

            product_nutrient_cols = {
                "energy-kcal_100g": "energy_kcal_100g",
                "energy-kj_100g": "energy_kj_100g",
                "energy_100g": "energy_kj_100g",
                "fat_100g": "fat_100g",
                "saturated-fat_100g": "saturated_fat_100g",
                "carbohydrates_100g": "carbohydrates_100g",
                "sugars_100g": "sugars_100g",
                "fiber_100g": "fiber_100g",
                "proteins_100g": "protein_100g",
                "sodium_100g": "sodium_100g",
                "salt_100g": "salt_100g",
            }

            with connect(
                database_url_env=args.database_url_env,
                application_name="foodb-openfoodfacts-import",
            ) as conn:
                with conn.cursor() as cur:
                    for stmt in schema_ddl(schema=args.schema):
                        cur.execute(stmt)
                    conn.commit()

                    if args.truncate:
                        cur.execute(
                            sql.SQL("TRUNCATE TABLE {}.nutrient_100g").format(
                                sql.Identifier(args.schema)
                            )
                        )
                        cur.execute(
                            sql.SQL("TRUNCATE TABLE {}.product_raw").format(
                                sql.Identifier(args.schema)
                            )
                        )
                        conn.commit()

                    product_cols = [
                        "code_norm",
                        "code_raw",
                        "product_name",
                        "brands",
                        "categories",
                        "quantity",
                        "serving_size",
                        "last_modified_t",
                        "energy_kcal_100g",
                        "energy_kj_100g",
                        "fat_100g",
                        "saturated_fat_100g",
                        "carbohydrates_100g",
                        "sugars_100g",
                        "fiber_100g",
                        "protein_100g",
                        "sodium_100g",
                        "salt_100g",
                    ]
                    product_numeric_cols = {
                        "last_modified_t",
                        "energy_kcal_100g",
                        "energy_kj_100g",
                        "fat_100g",
                        "saturated_fat_100g",
                        "carbohydrates_100g",
                        "sugars_100g",
                        "fiber_100g",
                        "protein_100g",
                        "sodium_100g",
                        "salt_100g",
                    }
                    copy_product = sql.SQL("COPY {}.product_raw ({}) FROM STDIN").format(
                        sql.Identifier(args.schema),
                        sql.SQL(", ").join(sql.Identifier(c) for c in product_cols),
                    )
                    copy_nutrient = sql.SQL(
                        "COPY {}.nutrient_100g (code_norm, nutrient_key, value, unit, source_field) FROM STDIN"
                    ).format(sql.Identifier(args.schema))

                    def copy_with_retry(
                        stmt: sql.SQL,
                        data: str,
                        *,
                        kind: str,
                        rows_products: int,
                    ) -> None:
                        for attempt in range(args.retries + 1):
                            try:
                                with cur.copy(stmt) as copy:
                                    copy.write(data)
                                conn.commit()
                                return
                            except psycopg.errors.SerializationFailure as e:
                                conn.rollback()
                                if attempt >= args.retries:
                                    raise
                                sleep_s = (
                                    min(args.retry_sleep_s * (2**attempt), 10.0)
                                    + random.random() * 0.05
                                )
                                logger.event(
                                    "retry",
                                    kind=kind,
                                    rows_products=rows_products,
                                    attempt=attempt + 1,
                                    sleep_s=round(sleep_s, 3),
                                    error_type=type(e).__name__,
                                )
                                time.sleep(sleep_s)
                            except Exception:
                                conn.rollback()
                                raise

                    def flush(
                        buf: io.StringIO, stmt: sql.SQL, rows_products: int, *, kind: str
                    ) -> io.StringIO:
                        data = buf.getvalue()
                        if not data:
                            return buf
                        copy_with_retry(stmt, data, kind=kind, rows_products=rows_products)
                        logger.event("chunk_commit", kind=kind, rows_products=rows_products)
                        return io.StringIO()

                    product_buf = io.StringIO()
                    nutrient_buf = io.StringIO()
                    rows_in_chunk = 0
                    products = 0
                    nutrients = 0
                    skipped_no_code = 0
                    skipped_duplicate_code = 0
                    seen_codes: set[str] | None = set() if args.dedupe == "memory" else None
                    duplicate_best: dict[str, tuple[tuple[int, int, int], str, list[str]]] = {}
                    duplicates_resolved = 0
                    product_score_fields = (
                        "product_name",
                        "brands",
                        "categories",
                        "quantity",
                        "serving_size",
                        "last_modified_t",
                    )

                    t0 = time.time()
                    reader = csv.reader(f, delimiter=delimiter)
                    for _row_number, row in enumerate(reader, start=2):
                        if args.max_rows and products >= args.max_rows:
                            break

                        raw_code = get(row, "code").strip()
                        code_norm = normalize_barcode(raw_code).normalized
                        if code_norm == "":
                            skipped_no_code += 1
                            continue
                        if seen_codes is not None:
                            if code_norm in seen_codes:
                                skipped_duplicate_code += 1
                                continue
                            seen_codes.add(code_norm)

                        base: dict[str, str] = {
                            "code_norm": code_norm,
                            "code_raw": raw_code,
                            "product_name": get(row, "product_name").strip(),
                            "brands": get(row, "brands").strip(),
                            "categories": get(row, "categories").strip(),
                            "quantity": get(row, "quantity").strip(),
                            "serving_size": get(row, "serving_size").strip(),
                            "last_modified_t": _int_or_empty(get(row, "last_modified_t")),
                            "energy_kcal_100g": "",
                            "energy_kj_100g": "",
                            "fat_100g": "",
                            "saturated_fat_100g": "",
                            "carbohydrates_100g": "",
                            "sugars_100g": "",
                            "fiber_100g": "",
                            "protein_100g": "",
                            "sodium_100g": "",
                            "salt_100g": "",
                        }

                        nutrient_lines: list[str] = []
                        nutrient_count = 0

                        for source_field in nutrient_fields:
                            val = get(row, source_field)
                            if val == "":
                                continue
                            cleaned = _float_or_empty(val)
                            if cleaned == "":
                                continue

                            if args.nutrients == "minimal":
                                nk, unit = by_field[source_field]
                            else:
                                nk = normalize_nutrient_key_from_field(source_field)
                                unit = unit_for_source_field(source_field)

                            col = product_nutrient_cols.get(source_field)
                            if col is not None:
                                if (
                                    col == "energy_kj_100g"
                                    and source_field == "energy_100g"
                                    and base[col] != ""
                                ):
                                    pass
                                else:
                                    base[col] = cleaned

                            if (
                                args.nutrients == "minimal"
                                and source_field == "energy_100g"
                                and get(row, "energy-kj_100g")
                            ):
                                continue

                            nutrient_lines.append(
                                "\t".join(
                                    _copy_cell(x, null_if_empty=False)
                                    for x in (code_norm, nk, cleaned, unit, source_field)
                                )
                                + "\n"
                            )
                            nutrient_count += 1

                        product_line = (
                            "\t".join(
                                _copy_cell(str(base[c]), null_if_empty=c in product_numeric_cols)
                                for c in product_cols
                            )
                            + "\n"
                        )

                        if duplicate_codes and code_norm in duplicate_codes:
                            last_modified = (
                                int(base["last_modified_t"]) if base["last_modified_t"] else -1
                            )
                            product_nonempty = sum(1 for k in product_score_fields if base[k] != "")
                            score = (last_modified, nutrient_count, product_nonempty)
                            existing = duplicate_best.get(code_norm)
                            if existing is None or score > existing[0]:
                                duplicate_best[code_norm] = (score, product_line, nutrient_lines)
                            continue

                        product_buf.write(product_line)
                        for line in nutrient_lines:
                            nutrient_buf.write(line)
                        products += 1
                        nutrients += nutrient_count
                        rows_in_chunk += 1

                        if rows_in_chunk >= args.chunk_rows:
                            product_buf = flush(
                                product_buf, copy_product, rows_in_chunk, kind="product_raw"
                            )
                            nutrient_buf = flush(
                                nutrient_buf, copy_nutrient, rows_in_chunk, kind="nutrient_100g"
                            )
                            rows_in_chunk = 0

                    if duplicate_best:
                        for _code_norm, (
                            _score,
                            product_line,
                            nutrient_lines,
                        ) in duplicate_best.items():
                            product_buf.write(product_line)
                            for line in nutrient_lines:
                                nutrient_buf.write(line)
                            products += 1
                            nutrients += len(nutrient_lines)
                            rows_in_chunk += 1
                            duplicates_resolved += 1
                            if rows_in_chunk >= args.chunk_rows:
                                product_buf = flush(
                                    product_buf, copy_product, rows_in_chunk, kind="product_raw"
                                )
                                nutrient_buf = flush(
                                    nutrient_buf, copy_nutrient, rows_in_chunk, kind="nutrient_100g"
                                )
                                rows_in_chunk = 0

                    if rows_in_chunk:
                        product_buf = flush(
                            product_buf, copy_product, rows_in_chunk, kind="product_raw"
                        )
                        nutrient_buf = flush(
                            nutrient_buf, copy_nutrient, rows_in_chunk, kind="nutrient_100g"
                        )

                    cur.execute(
                        sql.SQL(
                            "INSERT INTO {}.import_metadata (source_url, file_path, file_sha256, file_bytes, delimiter, nutrients_mode) "
                            "VALUES (%s, %s, %s, %s, %s, %s)"
                        ).format(sql.Identifier(args.schema)),
                        (
                            "https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz",
                            str(args.tsv_path),
                            file_sha256,
                            file_bytes,
                            delimiter,
                            args.nutrients,
                        ),
                    )
                    conn.commit()

                    if not args.skip_indexes:
                        for stmt in index_ddl(schema=args.schema):
                            cur.execute(stmt)
                        conn.commit()

                    logger.event(
                        "done",
                        products=products,
                        nutrients=nutrients,
                        skipped_no_code=skipped_no_code,
                        skipped_duplicate_code=skipped_duplicate_code,
                        duplicates_resolved=duplicates_resolved,
                        seconds=round(time.time() - t0, 2),
                    )

        return 0
    finally:
        logger.close()


if __name__ == "__main__":
    raise SystemExit(main())
