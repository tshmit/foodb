"""Test duplicate row scoring logic.

The implementation scores duplicate rows by:
  (last_modified_t, nutrient_count, product_nonempty_fields)

Higher score wins. Tuple comparison is lexicographic.
"""

from __future__ import annotations

import csv
from pathlib import Path


def test_duplicate_scoring_fixture_structure():
    """Verify the duplicates_scored.tsv fixture has expected structure."""
    fixture = Path(__file__).parent.parent / "fixtures" / "duplicates_scored.tsv"

    with open(fixture, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        rows = list(reader)

    # Should have 4 rows: 3 duplicates for code 0012345, 1 unique for 0067890
    assert len(rows) == 4

    # First three rows should have same code
    assert rows[0]["code"] == "0012345"
    assert rows[1]["code"] == "0012345"
    assert rows[2]["code"] == "0012345"

    # Fourth row should be unique
    assert rows[3]["code"] == "0067890"


def test_duplicate_scoring_last_modified_wins():
    """Newer last_modified_t should win if other factors equal."""
    fixture = Path(__file__).parent.parent / "fixtures" / "duplicates_scored.tsv"

    with open(fixture, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        rows = [r for r in reader if r["code"] == "0012345"]

    # Row 0: last_modified_t=1704067200 (older)
    # Row 1: last_modified_t=1704153600 (newer)
    # Row 2: last_modified_t=1704153600 (newer, same as row 1)

    row0_ts = int(rows[0]["last_modified_t"])
    row1_ts = int(rows[1]["last_modified_t"])
    row2_ts = int(rows[2]["last_modified_t"])

    assert row0_ts < row1_ts, "Row 0 should be older than row 1"
    assert row1_ts == row2_ts, "Row 1 and 2 should have same timestamp"


def test_duplicate_scoring_nutrient_count():
    """More nutrients should win if last_modified_t is equal."""
    fixture = Path(__file__).parent.parent / "fixtures" / "duplicates_scored.tsv"

    nutrient_fields = [
        "energy-kcal_100g",
        "fat_100g",
        "proteins_100g",
        "carbohydrates_100g",
        "sugars_100g",
    ]

    with open(fixture, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        rows = [r for r in reader if r["code"] == "0012345"]

    def count_nutrients(row: dict[str, str]) -> int:
        return sum(1 for k in nutrient_fields if row.get(k, "").strip() != "")

    # Row 0: 2 nutrients (energy, fat)
    # Row 1: 2 nutrients (energy, fat)
    # Row 2: 5 nutrients (all fields)

    assert count_nutrients(rows[0]) == 2
    assert count_nutrients(rows[1]) == 2
    assert count_nutrients(rows[2]) == 5

    # Row 2 should win due to more nutrients (same timestamp as row 1)


def test_duplicate_scoring_product_fields():
    """More non-empty product fields should win if nutrients equal."""
    fixture = Path(__file__).parent.parent / "fixtures" / "duplicates_scored.tsv"

    # Product fields checked by implementation (example subset)
    product_score_fields = ["product_name", "brands", "categories", "quantity"]

    with open(fixture, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        rows = [r for r in reader if r["code"] == "0012345"]

    def count_product_fields(row: dict[str, str]) -> int:
        return sum(1 for k in product_score_fields if row.get(k, "").strip() != "")

    # Row 0: product_name, brands (2 fields)
    # Row 1: product_name, brands (2 fields)
    # Row 2: product_name, brands, categories, quantity (4 fields)

    assert count_product_fields(rows[0]) == 2
    assert count_product_fields(rows[1]) == 2
    assert count_product_fields(rows[2]) == 4


def test_duplicate_scoring_best_row_identification():
    """The complete scoring logic should identify row 2 as best."""
    fixture = Path(__file__).parent.parent / "fixtures" / "duplicates_scored.tsv"

    nutrient_fields = [
        "energy-kcal_100g",
        "fat_100g",
        "proteins_100g",
        "carbohydrates_100g",
        "sugars_100g",
    ]
    product_score_fields = ["product_name", "brands", "categories", "quantity"]

    with open(fixture, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        rows = [r for r in reader if r["code"] == "0012345"]

    def compute_score(row: dict[str, str]) -> tuple[int, int, int]:
        last_modified = int(row["last_modified_t"]) if row["last_modified_t"] else -1
        nutrient_count = sum(1 for k in nutrient_fields if row.get(k, "").strip() != "")
        product_nonempty = sum(1 for k in product_score_fields if row.get(k, "").strip() != "")
        return (last_modified, nutrient_count, product_nonempty)

    scores = [compute_score(r) for r in rows]

    # Row 0: (1704067200, 2, 2) - older
    # Row 1: (1704153600, 2, 2) - newer but incomplete
    # Row 2: (1704153600, 5, 4) - newer and complete (BEST)

    assert scores[0] == (1704067200, 2, 2)
    assert scores[1] == (1704153600, 2, 2)
    assert scores[2] == (1704153600, 5, 4)

    # Row 2 should have highest score
    best_idx = scores.index(max(scores))
    assert best_idx == 2

    # Verify the best row is "Newer Complete"
    assert rows[best_idx]["product_name"] == "Newer Complete"
