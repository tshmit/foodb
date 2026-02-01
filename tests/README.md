# Test Suite for foodb

This directory contains tests for the foodb package, focusing on OpenFoodFacts import functionality.

## Structure

```
tests/
  unit/              # Fast unit tests, no external dependencies
    test_bom_handling.py
    test_delimiter_detection.py
    test_duplicate_scoring.py
  integration/       # Integration tests (may require mocking or temp DBs)
    test_manifest_validation.py
  fixtures/          # Test data files
    bom_utf8.tsv
    bom_utf8_with_bom.tsv
    comma_delimited.csv
    tab_delimited.tsv
    duplicates_scored.tsv
```

## Running Tests

### Install pytest

```bash
.venv/bin/pip install pytest
```

### Run all tests

```bash
.venv/bin/pytest tests/
```

### Run specific test file

```bash
.venv/bin/pytest tests/unit/test_bom_handling.py
```

### Run with verbose output

```bash
.venv/bin/pytest tests/ -v
```

### Run specific test function

```bash
.venv/bin/pytest tests/unit/test_bom_handling.py::test_bom_stripped_from_header
```

## Test Coverage

### BOM Handling (`test_bom_handling.py`)
- Verifies UTF-8 BOM is stripped from header line
- Ensures first column is parsed as "code" not "\ufeffcode"
- Tests row parsing works correctly after BOM removal
- Tests files without BOM work normally

### Delimiter Detection (`test_delimiter_detection.py`)
- Tests auto-detection of tab vs comma delimiters
- Verifies override behavior when `--delimiter` is explicit
- Tests edge cases (no delimiters, equal counts)

### Duplicate Scoring (`test_duplicate_scoring.py`)
- Verifies scoring logic: `(last_modified_t, nutrient_count, product_nonempty_fields)`
- Tests that newer `last_modified_t` wins
- Tests that more nutrients win when timestamps equal
- Tests that more product fields win when nutrients equal
- Validates the best row is selected correctly

### Manifest Validation (`test_manifest_validation.py`)
- Tests manifest structure validation
- Tests SHA-256 mismatch detection
- Tests byte-size mismatch detection
- Tests duplicate gating logic
- Tests missing/invalid field handling

## Fixtures

### `bom_utf8_with_bom.tsv`
Tab-delimited file with UTF-8 BOM (0xEF 0xBB 0xBF) at start. Tests BOM handling.

### `duplicates_scored.tsv`
Contains 3 rows with code `0012345` with varying quality:
- Row 0: old timestamp, incomplete
- Row 1: newer timestamp, still incomplete
- Row 2: newer timestamp, complete (should win)

### `comma_delimited.csv` / `tab_delimited.tsv`
Identical data in different delimiter formats. Tests delimiter detection.

## Adding New Tests

1. Create fixture data in `tests/fixtures/` if needed
2. Add test file in `tests/unit/` or `tests/integration/`
3. Import pytest: `import pytest`
4. Use descriptive test names: `test_<what_is_being_tested>`
5. Add docstrings explaining what behavior is proven
6. Run tests to verify they pass

## CI Integration

To integrate with CI, add to your workflow:

```yaml
- name: Run tests
  run: |
    .venv/bin/pip install pytest
    .venv/bin/pytest tests/ -v
```
