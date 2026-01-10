#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import psycopg
from psycopg import sql


def _parse_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8", errors="strict").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        values[key] = value
    return values


def _database_url() -> str:
    if "DATABASE_URL" in os.environ and os.environ["DATABASE_URL"].strip():
        return os.environ["DATABASE_URL"].strip()
    env = _parse_env_file(Path(".env"))
    url = env.get("DATABASE_URL", "").strip()
    if not url:
        raise SystemExit("Missing DATABASE_URL (set env var or add to .env).")
    return url


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


_NON_ALNUM = re.compile(r"[^a-z0-9]+")
_INT_RE = re.compile(r"^-?\d+$")
_FLOAT_RE = re.compile(r"^-?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE]-?\d+)?$")
_DATE_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DATE_SLASH_RE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")


def _normalize_date(value: str) -> str:
    value = value.strip()
    if _DATE_ISO_RE.match(value):
        return value
    m = _DATE_SLASH_RE.match(value)
    if m:
        month, day, year = m.groups()
        return f"{year}-{int(month):02d}-{int(day):02d}"
    raise ValueError(f"invalid DATE value {value!r}")


def _normalize_identifier(raw: str) -> str:
    value = raw.strip().lower()
    value = value.replace("\ufeff", "")  # UTF-8 BOM, just in case
    value = value.replace(".", "_")
    value = _NON_ALNUM.sub("_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    if not value:
        value = "col"
    if value[0].isdigit():
        value = f"c_{value}"
    return value


def _unique_identifiers(raw_headers: list[str]) -> list[str]:
    used: dict[str, int] = {}
    out: list[str] = []
    for raw in raw_headers:
        base = _normalize_identifier(raw)
        n = used.get(base, 0)
        if n == 0:
            used[base] = 1
            out.append(base)
        else:
            used[base] = n + 1
            out.append(f"{base}_{n+1}")
    return out


def _maybe_extract_dictionary_text(pdf_path: Path) -> str | None:
    try:
        from pypdf import PdfReader  # type: ignore
    except Exception:
        return None
    try:
        reader = PdfReader(str(pdf_path))
        parts: list[str] = []
        for page in reader.pages:
            parts.append(page.extract_text() or "")
        return "\n".join(parts)
    except Exception:
        return None


def _dictionary_coverage_report(
    dictionary_text: str, *, table: str, raw_headers: Iterable[str]
) -> tuple[bool, list[str]]:
    normalized_text = dictionary_text.replace("\u00a0", " ")
    found_table = bool(re.search(rf"\b{re.escape(table)}\b", normalized_text))
    missing: list[str] = []
    for header in raw_headers:
        token = header.strip()
        if not token:
            continue
        if token not in normalized_text:
            missing.append(token)
    return found_table, missing


def _column_type(table: str, column: str) -> str:
    # In recent USDA releases, `food.csv`'s `food_category_id` is not consistently an integer FK;
    # it's often a branded category name (e.g., "Oils Edible"), but can still be numeric for
    # SR Legacy foods. Store as STRING to handle both.
    if table == "food" and column == "food_category_id":
        return "STRING"

    date_cols = {
        "acquisition_date",
        "available_date",
        "discontinued_date",
        "end_date",
        "expiration_date",
        "last_updated",
        "modified_date",
        "publication_date",
        "sell_by_date",
        "start_date",
    }
    float_cols = {
        "adjusted_amount",
        "amount",
        "carbohydrate_value",
        "fat_value",
        "gram_weight",
        "loq",
        "max",
        "median",
        "min",
        "nutrient_nbr",
        "nutrient_value",
        "pct_weight",
        "percent_daily_value",
        "protein_value",
        "rank",
        "serving_size",
        "value",
    }
    int_cols = {
        "data_points",
        "food_group_id",
        "min_year_acquired",
        "seq_num",
        "sr_addmod_year",
        "wweia_category_code",
    }
    text_force = {
        "gtin_upc",
        "ndb_number",
        "upc_code",
    }

    if column in date_cols or column.endswith("_date"):
        return "DATE"
    if column in text_force:
        return "STRING"
    if column in float_cols:
        return "FLOAT8"
    if column in int_cols:
        return "INT8"

    # Common IDs (keep as INT8) unless they are clearly codes.
    if column == "id" or column.endswith("_id") or column in {"fdc_id", "foodid"}:
        if column.endswith("_code") or column.endswith("_number") or column.endswith("_nbr"):
            return "STRING"
        return "INT8"

    return "STRING"


@dataclass(frozen=True)
class TableSpec:
    table: str
    csv_path: Path
    raw_headers: list[str]
    columns: list[str]
    primary_key: list[str] | None


def _read_csv_header(csv_path: Path) -> list[str]:
    with csv_path.open("r", encoding="utf-8", errors="strict", newline="") as f:
        reader = csv.reader(f)
        try:
            return next(reader)
        except StopIteration as e:
            raise ValueError(f"Empty CSV: {csv_path}") from e


def _count_csv_data_rows(csv_path: Path) -> int:
    with csv_path.open("r", encoding="utf-8", errors="strict", newline="") as f:
        reader = csv.reader(f)
        try:
            next(reader)
        except StopIteration:
            return 0
        return sum(1 for _ in reader)


def _primary_key_for(table: str, columns: list[str]) -> list[str] | None:
    if "id" in columns:
        return ["id"]
    if "fdc_id" in columns:
        # Most per-food extension tables are 1:1 with food.
        if table in {"branded_food", "foundation_food", "sr_legacy_food", "survey_fndds_food"}:
            return ["fdc_id"]
        if columns == ["fdc_id"]:
            return ["fdc_id"]
    if table == "acquisition_samples":
        return ["fdc_id_of_sample_food", "fdc_id_of_acquisition_food"]
    if table == "lab_method_code":
        return ["lab_method_id", "code"]
    if table == "lab_method_nutrient":
        return ["lab_method_id", "nutrient_id"]
    if table == "sub_sample_result":
        return ["food_nutrient_id", "lab_method_id"]
    if table == "market_acquisition" and "acquisition_number" in columns and "fdc_id" in columns:
        return ["fdc_id", "acquisition_number"]
    if (
        table == "food_calorie_conversion_factor"
        and "food_nutrient_conversion_factor_id" in columns
    ):
        return ["food_nutrient_conversion_factor_id"]
    if (
        table == "food_protein_conversion_factor"
        and "food_nutrient_conversion_factor_id" in columns
    ):
        return ["food_nutrient_conversion_factor_id"]
    return None


def _table_specs(csv_dir: Path) -> list[TableSpec]:
    csv_paths = sorted(p for p in csv_dir.glob("*.csv") if p.is_file())
    specs: list[TableSpec] = []
    for csv_path in csv_paths:
        if csv_path.name == "all_downloaded_table_record_counts.csv":
            continue
        table = _normalize_identifier(csv_path.stem)
        raw_headers = _read_csv_header(csv_path)
        columns = _unique_identifiers(raw_headers)
        pk = _primary_key_for(table, columns)
        specs.append(
            TableSpec(
                table=table,
                csv_path=csv_path,
                raw_headers=raw_headers,
                columns=columns,
                primary_key=pk,
            )
        )
    return specs


def _ddl_for_table(schema: str, spec: TableSpec) -> str:
    col_lines = []
    for col in spec.columns:
        col_type = _column_type(spec.table, col)
        col_lines.append(f"  {_quote_ident(col)} {col_type}")
    pk_line = ""
    if spec.primary_key:
        pk_cols = ", ".join(_quote_ident(c) for c in spec.primary_key)
        pk_line = f",\n  PRIMARY KEY ({pk_cols})"
    return (
        f"CREATE TABLE IF NOT EXISTS {_quote_ident(schema)}.{_quote_ident(spec.table)} (\n"
        + ",\n".join(col_lines)
        + pk_line
        + "\n)"
    )


def _reorder_specs(specs: list[TableSpec], only_args: list[str]) -> list[TableSpec]:
    if not only_args:
        return specs
    order: list[str] = []
    seen: set[str] = set()
    for raw in only_args:
        name = _normalize_identifier(raw)
        if name not in seen:
            seen.add(name)
            order.append(name)
    index = {name: i for i, name in enumerate(order)}

    def sort_key(spec: TableSpec) -> tuple[int, str]:
        return (index.get(spec.table, 1_000_000), spec.table)

    return sorted(specs, key=sort_key)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Import USDA FoodData Central CSV bundle into CockroachDB."
    )
    parser.add_argument(
        "--csv-dir",
        type=Path,
        default=Path("usda/FoodData_Central_csv_2025-12-18"),
        help="Directory containing the USDA CSV files.",
    )
    parser.add_argument("--schema", default="usda", help="Target schema name.")
    parser.add_argument(
        "--only",
        action="append",
        default=[],
        help="Import only these tables (repeatable, matches CSV filename stem).",
    )
    parser.add_argument(
        "--skip",
        action="append",
        default=[],
        help="Skip these tables (repeatable, matches CSV filename stem).",
    )
    parser.add_argument(
        "--drop-schema",
        action="store_true",
        help="Drop and recreate the target schema before import (destructive).",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate each table before loading it (avoids duplicate rows on re-run).",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume by skipping already-loaded rows (uses current table row count; safest after an interrupted run).",
    )
    parser.add_argument(
        "--skip-indexes",
        action="store_true",
        help="Skip creating post-import indexes (saves RUs; you can add indexes later).",
    )
    parser.add_argument(
        "--benchmark",
        action="store_true",
        help="Print throughput and extrapolated full-import time (best with a single --only table).",
    )
    parser.add_argument(
        "--chunk-rows",
        type=int,
        default=200_000,
        help="Rows per COPY transaction (CockroachDB has transaction lock limits for very large COPY statements).",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Optional log file to append events to.",
    )
    parser.add_argument(
        "--log-format",
        choices=["text", "jsonl"],
        default="text",
        help="Log format for --log-file (and stdout).",
    )
    parser.add_argument(
        "--progress-every-s",
        type=float,
        default=10.0,
        help="Emit periodic progress events while loading.",
    )
    parser.add_argument(
        "--dictionary-pdf",
        type=Path,
        default=Path("usda/Download_Field_Descriptions_Oct2020.pdf"),
        help="USDA data dictionary PDF (used for coverage warnings).",
    )
    parser.add_argument("--retries", type=int, default=0, help="Per-table retry count.")
    parser.add_argument(
        "--retry-sleep-s", type=float, default=2.0, help="Seconds to sleep between retries."
    )
    args = parser.parse_args(argv)

    if args.chunk_rows < 1_000:
        raise SystemExit("--chunk-rows must be >= 1000 to avoid excessive transaction overhead.")
    if args.resume and (args.drop_schema or args.truncate):
        raise SystemExit("--resume cannot be used with --drop-schema or --truncate.")
    if args.resume and args.retries != 0:
        raise SystemExit(
            "--resume requires --retries 0 (retry truncation would invalidate resume offsets)."
        )

    database_url = _database_url()
    if not args.csv_dir.exists():
        raise SystemExit(f"CSV directory not found: {args.csv_dir}")

    specs = _table_specs(args.csv_dir)
    only = {_normalize_identifier(t) for t in args.only}
    skip = {_normalize_identifier(t) for t in args.skip}
    if only:
        specs = [s for s in specs if s.table in only]
    if skip:
        specs = [s for s in specs if s.table not in skip]
    specs = _reorder_specs(specs, args.only)
    if not specs:
        raise SystemExit(f"No CSV files found in: {args.csv_dir}")

    # Optional dictionary coverage check (PDF is Oct 2020 and may not match newer releases).
    dictionary_text = None
    if args.dictionary_pdf.exists():
        dictionary_text = _maybe_extract_dictionary_text(args.dictionary_pdf)
    if dictionary_text:
        missing_tables = [s.table for s in specs if s.table not in dictionary_text]
        if missing_tables:
            print(
                f"Warning: {len(missing_tables)} tables not found in dictionary PDF (likely newer than Oct 2020).",
                file=sys.stderr,
            )

    record_counts_path = args.csv_dir / "all_downloaded_table_record_counts.csv"
    expected_rows: dict[str, int] = {}
    if record_counts_path.exists():
        with record_counts_path.open("r", encoding="utf-8", errors="strict", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                name = row["Table"]
                if not name.endswith(".csv"):
                    continue
                table = _normalize_identifier(name.replace(".csv", ""))
                expected_rows[table] = int(row["Number of Records"])
        expected_rows.pop("all_downloaded_table_record_counts", None)

    def _fmt_duration(seconds: float) -> str:
        seconds = max(0.0, seconds)
        if seconds < 90:
            return f"{seconds:.1f}s"
        return f"{seconds/60:.1f}m"

    class Logger:
        def __init__(self, *, log_file: Path | None, fmt: str) -> None:
            self._fmt = fmt
            self._fh = None
            if log_file is not None:
                log_file.parent.mkdir(parents=True, exist_ok=True)
                self._fh = log_file.open("a", encoding="utf-8", errors="strict")

        def close(self) -> None:
            if self._fh is not None:
                self._fh.close()

        def event(self, name: str, **fields: object) -> None:
            payload = {
                "ts": datetime.now(UTC).isoformat(timespec="seconds"),
                "event": name,
                **fields,
            }
            if self._fmt == "jsonl":
                line = json.dumps(payload, ensure_ascii=False)
            else:
                kv = " ".join(f"{k}={payload[k]}" for k in payload if k not in {"ts", "event"})
                line = f"[{payload['ts']}] {name}" + (f" {kv}" if kv else "")
            print(line, flush=True)
            if self._fh is not None:
                self._fh.write(line + "\n")
                self._fh.flush()

    logger = Logger(log_file=args.log_file, fmt=args.log_format)

    selected_csv_bytes = sum(s.csv_path.stat().st_size for s in specs)
    selected_expected_rows = sum(expected_rows.get(s.table, 0) for s in specs)
    bundle_csv_paths = [
        p for p in args.csv_dir.glob("*.csv") if p.name != "all_downloaded_table_record_counts.csv"
    ]
    bundle_total_bytes = sum(p.stat().st_size for p in bundle_csv_paths)
    bundle_total_rows = sum(
        v for k, v in expected_rows.items() if k != "all_downloaded_table_record_counts"
    )

    t0 = time.time()
    overall_loaded_rows = 0
    baseline_loaded_rows = 0
    overall_known_total = selected_expected_rows
    overall_progress_next = time.monotonic() + args.progress_every_s

    logger.event(
        "run_start",
        schema=args.schema,
        tables=len(specs),
        csv_dir=str(args.csv_dir),
        chunk_rows=args.chunk_rows,
        expected_rows_total=overall_known_total,
        bytes_total=selected_csv_bytes,
    )

    run_success = False
    try:
        with psycopg.connect(database_url, application_name="foodb-usda-import") as conn:
            with conn.cursor() as cur:
                if args.drop_schema:
                    logger.event("schema_drop", schema=args.schema)
                    cur.execute(
                        sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                            sql.Identifier(args.schema)
                        )
                    )
                    conn.commit()
                cur.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(args.schema))
                )
                conn.commit()

                for spec in specs:
                    cur.execute(_ddl_for_table(args.schema, spec))
                conn.commit()

                existing_rows_by_table: dict[str, int] = {}
                if args.resume:
                    for spec in specs:
                        try:
                            cur.execute(
                                sql.SQL("SELECT count(*) FROM {}.{}").format(
                                    sql.Identifier(args.schema),
                                    sql.Identifier(spec.table),
                                )
                            )
                            existing_rows_by_table[spec.table] = int(cur.fetchone()[0])
                        except psycopg.errors.UndefinedTable:
                            existing_rows_by_table[spec.table] = 0

                    for spec in specs:
                        existing = existing_rows_by_table.get(spec.table, 0)
                        expected = expected_rows.get(spec.table)
                        if expected is not None and existing > expected:
                            csv_rows = _count_csv_data_rows(spec.csv_path)
                            logger.event(
                                "expected_rows_mismatch",
                                table=spec.table,
                                expected_rows_file=expected,
                                csv_rows=csv_rows,
                                existing_rows=existing,
                            )
                            expected_rows[spec.table] = csv_rows
                            if existing > csv_rows:
                                raise SystemExit(
                                    f"{args.schema}.{spec.table} has {existing:,} rows but CSV has only {csv_rows:,}"
                                )

                    overall_loaded_rows = sum(existing_rows_by_table.values())
                    baseline_loaded_rows = overall_loaded_rows
                    if overall_loaded_rows:
                        logger.event("run_resume", existing_rows=overall_loaded_rows)

                for spec in specs:
                    if args.truncate:
                        logger.event("table_truncate", table=spec.table)
                        cur.execute(
                            sql.SQL("TRUNCATE TABLE {}.{}").format(
                                sql.Identifier(args.schema), sql.Identifier(spec.table)
                            )
                        )
                        conn.commit()

                    expected = expected_rows.get(spec.table)
                    size_mib = spec.csv_path.stat().st_size / 1024 / 1024
                    logger.event(
                        "table_start",
                        table=spec.table,
                        expected_rows=expected,
                        size_mib=round(size_mib, 2),
                    )

                    existing = existing_rows_by_table.get(spec.table, 0) if args.resume else 0
                    if (
                        args.resume
                        and expected is not None
                        and existing == expected
                        and expected != 0
                    ):
                        logger.event("table_already_loaded", table=spec.table, rows=existing)
                        continue
                    if args.resume and existing:
                        logger.event("table_resume", table=spec.table, existing_rows=existing)

                    if dictionary_text:
                        found_table, missing = _dictionary_coverage_report(
                            dictionary_text, table=spec.table, raw_headers=spec.raw_headers
                        )
                        if not found_table:
                            logger.event("dictionary_miss_table", table=spec.table)
                        elif missing:
                            logger.event(
                                "dictionary_miss_headers",
                                table=spec.table,
                                missing_headers=len(missing),
                            )

                    attempt = 0
                    table_committed_rows = 0
                    while True:
                        try:
                            table_t0 = time.time()

                            # COPY in chunks to avoid CockroachDB lock-intent budget limits.
                            cols_sql = sql.SQL(", ").join(sql.Identifier(c) for c in spec.columns)
                            copy_stmt = sql.SQL("COPY {}.{} ({}) FROM STDIN").format(
                                sql.Identifier(args.schema),
                                sql.Identifier(spec.table),
                                cols_sql,
                            )

                            rows_in_chunk = 0
                            buf = io.StringIO()

                            def escape_copy_text(value: str) -> str:
                                return (
                                    value.replace("\\", "\\\\")
                                    .replace("\t", "\\t")
                                    .replace("\n", "\\n")
                                    .replace("\r", "\\r")
                                    .replace("\b", "\\b")
                                    .replace("\f", "\\f")
                                    .replace("\v", "\\v")
                                )

                            def flush() -> None:
                                nonlocal \
                                    rows_in_chunk, \
                                    buf, \
                                    table_committed_rows, \
                                    overall_loaded_rows
                                data = buf.getvalue()
                                if not data:
                                    return
                                with cur.copy(copy_stmt) as copy:
                                    copy.write(data)
                                conn.commit()

                                table_committed_rows += rows_in_chunk
                                overall_loaded_rows += rows_in_chunk
                                logger.event(
                                    "chunk_commit",
                                    table=spec.table,
                                    rows=rows_in_chunk,
                                    table_rows_done=table_committed_rows,
                                    overall_rows_done=overall_loaded_rows,
                                )
                                rows_in_chunk = 0
                                buf = io.StringIO()

                            column_types = [_column_type(spec.table, c) for c in spec.columns]
                            if args.resume:
                                table_committed_rows = existing

                            with spec.csv_path.open(
                                "r", encoding="utf-8", errors="strict", newline=""
                            ) as f:
                                reader = csv.reader(f)
                                try:
                                    next(reader)
                                except StopIteration as e:
                                    raise ValueError(f"Empty CSV: {spec.csv_path}") from e

                                expected_cols = len(spec.columns)
                                if args.resume and existing:
                                    for _ in range(existing):
                                        try:
                                            next(reader)
                                        except StopIteration:
                                            break
                                for row_number, row in enumerate(reader, start=2 + existing):
                                    if len(row) != expected_cols:
                                        raise ValueError(
                                            f"{spec.csv_path} row {row_number}: expected {expected_cols} columns, got {len(row)}"
                                        )

                                    cleaned_row: list[str] = []
                                    for col_name, col_type, value in zip(
                                        spec.columns, column_types, row, strict=True
                                    ):
                                        if value == "":
                                            cleaned_row.append("")
                                            continue
                                        if col_type in {"INT8", "FLOAT8", "DATE"}:
                                            value = value.strip()
                                        cleaned_row.append(value)
                                        if "\x00" in value:
                                            raise ValueError(
                                                f"{spec.csv_path} row {row_number} col {col_name}: NUL byte not allowed"
                                            )
                                        if col_type == "INT8" and not _INT_RE.match(value):
                                            raise ValueError(
                                                f"{spec.csv_path} row {row_number} col {col_name}: invalid INT8 value {value!r}"
                                            )
                                        if col_type == "FLOAT8" and not _FLOAT_RE.match(value):
                                            raise ValueError(
                                                f"{spec.csv_path} row {row_number} col {col_name}: invalid FLOAT8 value {value!r}"
                                            )
                                        if col_type == "DATE":
                                            try:
                                                cleaned_row[-1] = _normalize_date(value)
                                            except ValueError as e:
                                                raise ValueError(
                                                    f"{spec.csv_path} row {row_number} col {col_name}: {e}"
                                                ) from e

                                    out_fields: list[str] = []
                                    for v in cleaned_row:
                                        if v == "":
                                            out_fields.append("\\N")
                                        else:
                                            out_fields.append(escape_copy_text(v))
                                    buf.write("\t".join(out_fields))
                                    buf.write("\n")
                                    rows_in_chunk += 1

                                    now = time.monotonic()
                                    if now >= overall_progress_next:
                                        elapsed = time.time() - t0
                                        pct = (
                                            (overall_loaded_rows / overall_known_total) * 100.0
                                            if overall_known_total
                                            else None
                                        )
                                        run_rows = overall_loaded_rows - baseline_loaded_rows
                                        eta_s = (
                                            (
                                                (overall_known_total - overall_loaded_rows)
                                                / (run_rows / elapsed)
                                            )
                                            if overall_known_total and run_rows and elapsed > 0
                                            else None
                                        )
                                        logger.event(
                                            "progress",
                                            table=spec.table,
                                            table_rows_done=table_committed_rows + rows_in_chunk,
                                            table_rows_expected=expected,
                                            overall_rows_done=overall_loaded_rows,
                                            overall_rows_expected=overall_known_total,
                                            overall_pct=round(pct, 2) if pct is not None else None,
                                            eta=_fmt_duration(eta_s) if eta_s is not None else None,
                                        )
                                        overall_progress_next = now + args.progress_every_s

                                    if rows_in_chunk >= args.chunk_rows:
                                        flush()

                                flush()

                            dt = time.time() - table_t0
                            logger.event("table_done", table=spec.table, seconds=round(dt, 2))
                            break
                        except Exception:
                            conn.rollback()
                            attempt += 1
                            logger.event(
                                "table_error",
                                table=spec.table,
                                attempt=attempt,
                                error_type=type(sys.exc_info()[1]).__name__,
                                error=str(sys.exc_info()[1]),
                            )
                            if attempt > args.retries:
                                raise
                            # If we retry, clear any committed rows from this table to avoid duplicates.
                            if table_committed_rows:
                                try:
                                    cur.execute(
                                        sql.SQL("TRUNCATE TABLE {}.{}").format(
                                            sql.Identifier(args.schema), sql.Identifier(spec.table)
                                        )
                                    )
                                    conn.commit()
                                except Exception as cleanup_error:
                                    conn.rollback()
                                    logger.event(
                                        "table_retry_cleanup_failed",
                                        table=spec.table,
                                        error_type=type(cleanup_error).__name__,
                                        error=str(cleanup_error),
                                    )
                                    raise
                                overall_loaded_rows -= table_committed_rows
                                table_committed_rows = 0
                                logger.event("table_truncate_for_retry", table=spec.table)
                            time.sleep(args.retry_sleep_s)

                if not args.skip_indexes:
                    created_tables = {s.table for s in specs}
                    if "food" in created_tables:
                        cur.execute(
                            sql.SQL(
                                "CREATE INDEX IF NOT EXISTS food_description_idx ON {}.{} ({})"
                            ).format(
                                sql.Identifier(args.schema),
                                sql.Identifier("food"),
                                sql.Identifier("description"),
                            )
                        )
                    if "branded_food" in created_tables:
                        cur.execute(
                            sql.SQL(
                                "CREATE INDEX IF NOT EXISTS branded_food_gtin_upc_idx ON {}.{} ({})"
                            ).format(
                                sql.Identifier(args.schema),
                                sql.Identifier("branded_food"),
                                sql.Identifier("gtin_upc"),
                            )
                        )
                    conn.commit()
        run_success = True
    except KeyboardInterrupt:
        logger.event("run_interrupted")
        raise
    finally:
        elapsed_s = time.time() - t0
        if run_success and not args.benchmark:
            logger.event("run_done", seconds=round(elapsed_s, 2))
        logger.close()

    if args.benchmark:
        loaded_bytes = selected_csv_bytes
        loaded_rows = selected_expected_rows
        if len(specs) == 1:
            loaded_bytes = specs[0].csv_path.stat().st_size
            loaded_rows = expected_rows.get(specs[0].table, 0)

        bytes_per_s = loaded_bytes / max(elapsed_s, 1e-9)
        rows_per_s = loaded_rows / max(elapsed_s, 1e-9) if loaded_rows else 0.0
        est_by_bytes = bundle_total_bytes / max(bytes_per_s, 1e-9)
        est_by_rows = bundle_total_rows / max(rows_per_s, 1e-9) if rows_per_s else 0.0

        print(
            "\n".join(
                [
                    f"Benchmark elapsed: {_fmt_duration(elapsed_s)}",
                    f"Throughput: {bytes_per_s/1024/1024:.2f} MiB/s",
                    f"Throughput: {rows_per_s:,.0f} rows/s"
                    if loaded_rows
                    else "Throughput: (rows/s unavailable)",
                    f"Extrapolated full import (bytes): {_fmt_duration(est_by_bytes)}",
                    f"Extrapolated full import (rows): {_fmt_duration(est_by_rows)}"
                    if bundle_total_rows and rows_per_s
                    else "Extrapolated full import (rows): (unavailable)",
                ]
            )
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
