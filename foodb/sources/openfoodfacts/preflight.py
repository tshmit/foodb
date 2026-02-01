from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import os
import subprocess
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

from foodb.db.logging import Logger
from foodb.normalize.barcode import normalize_barcode


def _detect_delimiter(header_line: str) -> str:
    tabs = header_line.count("\t")
    commas = header_line.count(",")
    if commas > tabs and commas > 0:
        return ","
    return "\t"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Preflight OpenFoodFacts TSV/CSV export: stream code_norm and detect duplicates."
    )
    parser.add_argument(
        "--tsv-path",
        type=Path,
        required=True,
        help="Path to OFF export (.csv/.tsv, optionally .gz).",
    )
    parser.add_argument("--delimiter", default="\t", help="Field delimiter (default: tab).")
    parser.add_argument(
        "--manifest-out",
        type=Path,
        required=True,
        help="Write preflight manifest JSON to this path.",
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
        "--duplicate-samples",
        type=int,
        default=20,
        help="How many duplicate code_norm samples to include in the manifest.",
    )
    parser.add_argument(
        "--duplicate-codes-out",
        type=Path,
        default=None,
        help="Write unique duplicate code_norm values to this path (one per line).",
    )
    parser.add_argument(
        "--sort-tmp-dir",
        type=Path,
        default=None,
        help="Temporary directory for external sort spill files.",
    )
    parser.add_argument("--log-file", type=Path, default=None, help="Optional JSONL/text log file.")
    parser.add_argument(
        "--log-format", choices=["text", "jsonl"], default="text", help="Log output format."
    )
    return parser.parse_args(argv)


def _write_manifest(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", errors="strict") as f:
        json.dump(payload, f, ensure_ascii=True, indent=2, sort_keys=True)
        f.write("\n")


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logger = Logger(fmt=args.log_format, log_file=args.log_file)
    unsorted_path: Path | None = None
    sorted_path: Path | None = None
    duplicate_fh = None
    try:
        if not args.tsv_path.exists():
            raise SystemExit(f"Missing input: {args.tsv_path}")

        try:
            csv.field_size_limit(args.field_size_limit)
        except OverflowError:
            raise SystemExit(f"--field-size-limit is too large: {args.field_size_limit}") from None

        file_sha256 = _sha256(args.tsv_path)
        file_bytes = args.tsv_path.stat().st_size

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

            if "code" not in header_to_index:
                raise SystemExit("Input is missing required column: code")

            def get(row: list[str], name: str) -> str:
                idx = header_to_index.get(name)
                if idx is None or idx >= len(row):
                    return ""
                return row[idx]

            codes_total = 0
            skipped_no_code = 0

            with tempfile.NamedTemporaryFile(
                "w", delete=False, encoding="utf-8", errors="strict"
            ) as tmp:
                unsorted_path = Path(tmp.name)
                reader = csv.reader(f, delimiter=delimiter)
                for _row_number, row in enumerate(reader, start=2):
                    raw_code = get(row, "code").strip()
                    code_norm = normalize_barcode(raw_code).normalized
                    if code_norm == "":
                        skipped_no_code += 1
                        continue
                    tmp.write(code_norm + "\n")
                    codes_total += 1

        sorted_path = unsorted_path.with_suffix(".sorted")
        cmd = ["sort"]
        if args.sort_tmp_dir is not None:
            cmd.extend(["-T", str(args.sort_tmp_dir)])
        cmd.extend(["-o", str(sorted_path), str(unsorted_path)])
        env = os.environ.copy()
        env["LC_ALL"] = "C"
        t0 = time.time()
        try:
            subprocess.run(cmd, check=True, env=env)
        except FileNotFoundError:
            raise SystemExit("Missing required external command: sort") from None
        except subprocess.CalledProcessError as e:
            raise SystemExit(f"External sort failed with exit code {e.returncode}") from None

        duplicate_values = 0
        duplicate_occurrences = 0
        duplicate_samples: list[str] = []
        duplicate_codes_count = 0
        prev: str | None = None
        prev_count = 0

        if args.duplicate_codes_out is not None:
            args.duplicate_codes_out.parent.mkdir(parents=True, exist_ok=True)
            duplicate_fh = args.duplicate_codes_out.open("w", encoding="utf-8", errors="strict")

        with sorted_path.open("r", encoding="utf-8", errors="strict") as f:
            for line in f:
                code = line.rstrip("\n")
                if code == prev:
                    duplicate_occurrences += 1
                    prev_count += 1
                    if prev_count == 2:
                        duplicate_values += 1
                        if duplicate_fh is not None:
                            duplicate_fh.write(code + "\n")
                            duplicate_codes_count += 1
                        if len(duplicate_samples) < args.duplicate_samples:
                            duplicate_samples.append(code)
                else:
                    prev = code
                    prev_count = 1

        unique_codes = codes_total - duplicate_occurrences
        duplicates_found = duplicate_values > 0
        elapsed = round(time.time() - t0, 2)

        payload: dict[str, object] = {
            "format_version": 1,
            "created_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "file_path": str(args.tsv_path),
            "file_bytes": file_bytes,
            "file_sha256": file_sha256,
            "delimiter": delimiter,
            "detected_delimiter": detected,
            "code_total": codes_total,
            "code_unique": unique_codes,
            "duplicate_values": duplicate_values,
            "duplicate_occurrences": duplicate_occurrences,
            "duplicates_found": duplicates_found,
            "duplicate_samples": duplicate_samples,
            "duplicate_codes_count": duplicate_codes_count,
            "duplicate_codes_path": str(args.duplicate_codes_out)
            if args.duplicate_codes_out is not None
            else None,
            "skipped_no_code": skipped_no_code,
            "sort_seconds": elapsed,
        }

        _write_manifest(args.manifest_out, payload)
        logger.event(
            "preflight_done",
            manifest=str(args.manifest_out),
            codes_total=codes_total,
            duplicates_found=duplicates_found,
            seconds=elapsed,
        )

        return 0
    finally:
        if unsorted_path is not None and unsorted_path.exists():
            unsorted_path.unlink()
        if sorted_path is not None and sorted_path.exists():
            sorted_path.unlink()
        if duplicate_fh is not None:
            duplicate_fh.close()
        logger.close()


if __name__ == "__main__":
    raise SystemExit(main())
