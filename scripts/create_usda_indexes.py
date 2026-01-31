#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import psycopg
from psycopg import sql


@dataclass(frozen=True)
class IndexSpec:
    name: str
    table: str
    columns: tuple[str, ...]


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create recommended indexes for the USDA FDC import schema.",
    )
    parser.add_argument("--schema", default="usda", help="Target schema name.")
    parser.add_argument(
        "--only",
        action="append",
        default=[],
        help="Create only these indexes (repeatable; values are index names printed by --list).",
    )
    parser.add_argument(
        "--skip",
        action="append",
        default=[],
        help="Skip these indexes (repeatable; values are index names printed by --list).",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List known index names and exit.",
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
        "--database-url-env",
        default="DATABASE_URL",
        help="Env var name containing the DB connection string (default: DATABASE_URL).",
    )
    return parser.parse_args(argv)


def _normalize(name: str) -> str:
    return name.strip().lower().replace("-", "_")


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


def _index_specs(schema: str) -> list[IndexSpec]:
    return [
        IndexSpec(
            name="food_description_idx",
            table="food",
            columns=("description",),
        ),
        IndexSpec(
            name="food_fdc_id_idx",
            table="food",
            columns=("fdc_id",),
        ),
        IndexSpec(
            name="branded_food_gtin_upc_idx",
            table="branded_food",
            columns=("gtin_upc",),
        ),
        # Critical for app lookups: nutrients by food.
        IndexSpec(
            name="food_nutrient_fdc_id_idx",
            table="food_nutrient",
            columns=("fdc_id",),
        ),
        # Common app query: portions by food.
        IndexSpec(
            name="food_portion_fdc_id_idx",
            table="food_portion",
            columns=("fdc_id",),
        ),
        # Useful for browsing/filtering survey foods.
        IndexSpec(
            name="survey_fndds_food_food_code_idx",
            table="survey_fndds_food",
            columns=("food_code",),
        ),
        IndexSpec(
            name="survey_fndds_food_wweia_category_code_idx",
            table="survey_fndds_food",
            columns=("wweia_category_code",),
        ),
    ]


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    only = {_normalize(x) for x in args.only}
    skip = {_normalize(x) for x in args.skip}

    specs = _index_specs(args.schema)
    if args.list:
        for spec in specs:
            print(spec.name)
        return 0

    if only:
        specs = [s for s in specs if _normalize(s.name) in only]
    if skip:
        specs = [s for s in specs if _normalize(s.name) not in skip]

    if not specs:
        raise SystemExit("No indexes selected (check --only/--skip or use --list).")

    db_url = os.environ.get(args.database_url_env)
    if not db_url:
        raise SystemExit(f"{args.database_url_env} is not set")

    logger = Logger(log_file=args.log_file, fmt=args.log_format)
    try:
        logger.event(
            "index_run_start",
            schema=args.schema,
            indexes=len(specs),
            selected=[s.name for s in specs],
        )

        with psycopg.connect(
            db_url, connect_timeout=10, application_name="foodb-usda-indexes"
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s
                      AND table_type = 'BASE TABLE'
                    """,
                    (args.schema,),
                )
                existing_tables = {r[0] for r in cur.fetchall()}

                for spec in specs:
                    if spec.table not in existing_tables:
                        logger.event("index_skip_missing_table", index=spec.name, table=spec.table)
                        continue

                    logger.event("index_start", index=spec.name, table=spec.table)
                    t0 = time.time()
                    stmt = sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}.{} ({})").format(
                        sql.Identifier(spec.name),
                        sql.Identifier(args.schema),
                        sql.Identifier(spec.table),
                        sql.SQL(", ").join(sql.Identifier(c) for c in spec.columns),
                    )
                    cur.execute(stmt)
                    conn.commit()
                    logger.event("index_done", index=spec.name, seconds=round(time.time() - t0, 2))

        logger.event("index_run_done")
        return 0
    finally:
        logger.close()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
