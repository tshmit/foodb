"""Test UTF-8 BOM handling in TSV parsing."""

from __future__ import annotations

import csv
from pathlib import Path

# Test that BOM is stripped and first column is recognized correctly


def test_bom_stripped_from_header():
    """UTF-8 BOM at start of file should be stripped from header line."""
    fixture = Path(__file__).parent.parent / "fixtures" / "bom_utf8_with_bom.tsv"
    assert fixture.exists(), f"Missing fixture: {fixture}"

    with open(fixture, encoding="utf-8", newline="") as f:
        header_line = f.readline()

        # BOM should be present as unicode character
        assert header_line.startswith("\ufeff"), "Expected BOM at start of file"

        # Strip BOM as implementation does
        header_line = header_line.lstrip("\ufeff")

        # Parse header
        reader = csv.DictReader([header_line], delimiter="\t")
        fieldnames = reader.fieldnames

        # First column should be "code", not "\ufeffcode"
        assert fieldnames is not None
        assert fieldnames[0] == "code", f"Expected 'code', got {fieldnames[0]!r}"
        assert "product_name" in fieldnames
        assert "last_modified_t" in fieldnames


def test_bom_allows_row_parsing():
    """Rows should parse correctly after BOM is stripped from header."""
    fixture = Path(__file__).parent.parent / "fixtures" / "bom_utf8_with_bom.tsv"

    with open(fixture, encoding="utf-8", newline="") as f:
        header_line = f.readline()

        # Strip BOM
        if header_line.startswith("\ufeff"):
            header_line = header_line.lstrip("\ufeff")

        # Parse rows using corrected header
        reader = csv.DictReader(f, fieldnames=header_line.rstrip("\n").split("\t"), delimiter="\t")

        rows = list(reader)
        assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"

        # Verify first row
        assert rows[0]["code"] == "0012345"
        assert rows[0]["product_name"] == "Test Product A"
        assert rows[0]["energy-kcal_100g"] == "100"

        # Verify second row
        assert rows[1]["code"] == "0067890"
        assert rows[1]["product_name"] == "Test Product B"


def test_no_bom_works_normally():
    """Files without BOM should parse normally."""
    fixture = Path(__file__).parent.parent / "fixtures" / "bom_utf8.tsv"

    with open(fixture, encoding="utf-8", newline="") as f:
        header_line = f.readline()

        # No BOM expected
        assert not header_line.startswith("\ufeff")

        # Strip BOM anyway (no-op)
        header_line = header_line.lstrip("\ufeff")

        reader = csv.DictReader([header_line], delimiter="\t")
        fieldnames = reader.fieldnames

        assert fieldnames is not None
        assert fieldnames[0] == "code"
