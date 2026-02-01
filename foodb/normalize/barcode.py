from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NormalizedBarcode:
    raw: str
    normalized: str
    digits: int


def normalize_barcode(raw: str) -> NormalizedBarcode:
    raw = raw.strip()
    digits_only = "".join(ch for ch in raw if ch.isdigit())
    return NormalizedBarcode(raw=raw, normalized=digits_only, digits=len(digits_only))
