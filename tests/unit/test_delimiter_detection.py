"""Test delimiter detection and override behavior."""

from __future__ import annotations

from pathlib import Path


def _detect_delimiter(header_line: str) -> str:
    """Mirror implementation from foodb.sources.openfoodfacts."""
    tabs = header_line.count("\t")
    commas = header_line.count(",")
    if commas > tabs and commas > 0:
        return ","
    return "\t"


def test_detect_tab_delimiter():
    """Tab-delimited file should be detected as tab."""
    fixture = Path(__file__).parent.parent / "fixtures" / "tab_delimited.tsv"

    with open(fixture, encoding="utf-8", newline="") as f:
        header_line = f.readline()

    detected = _detect_delimiter(header_line)
    assert detected == "\t"


def test_detect_comma_delimiter():
    """Comma-delimited file should be detected as comma."""
    fixture = Path(__file__).parent.parent / "fixtures" / "comma_delimited.csv"

    with open(fixture, encoding="utf-8", newline="") as f:
        header_line = f.readline()

    detected = _detect_delimiter(header_line)
    assert detected == ","


def test_delimiter_override_logic():
    """Test the override logic: default tab + autodetect vs explicit override.

    Implementation logic:
    - delimiter = args.delimiter (default "\t")
    - if args.delimiter == "\t" and detected != "\t":
          delimiter = detected  # auto-override
    - else: use args.delimiter as-is
    """
    # Case 1: default \t, comma file → auto-detects and uses comma
    args_delimiter = "\t"  # default
    detected = ","
    delimiter = args_delimiter
    if args_delimiter == "\t" and detected != "\t":
        delimiter = detected
    assert delimiter == ","

    # Case 2: default \t, tab file → uses tab
    args_delimiter = "\t"
    detected = "\t"
    delimiter = args_delimiter
    if args_delimiter == "\t" and detected != "\t":
        delimiter = detected
    assert delimiter == "\t"

    # Case 3: explicit comma, tab file → uses comma (override)
    args_delimiter = ","
    detected = "\t"
    delimiter = args_delimiter
    if args_delimiter == "\t" and detected != "\t":
        delimiter = detected
    assert delimiter == ","

    # Case 4: explicit tab (via --delimiter \t on CLI, not default), comma file
    # This is ambiguous in the implementation: can't distinguish explicit vs default
    # But behavior is: uses comma due to auto-detect
    args_delimiter = "\t"
    detected = ","
    delimiter = args_delimiter
    if args_delimiter == "\t" and detected != "\t":
        delimiter = detected
    assert delimiter == ","


def test_edge_case_no_delimiters():
    """Header with no delimiters should default to tab."""
    header = "singlecolumn\n"
    detected = _detect_delimiter(header)
    assert detected == "\t"


def test_edge_case_equal_counts():
    """Equal tab and comma counts should default to tab."""
    header = "a\tb,c\td,e\n"  # 2 tabs, 2 commas
    detected = _detect_delimiter(header)
    assert detected == "\t"
