from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NutrientSpec:
    source_field: str
    nutrient_key: str
    unit: str


def minimal_nutrients(*, include_salt: bool) -> list[NutrientSpec]:
    specs = [
        NutrientSpec(source_field="energy-kcal_100g", nutrient_key="energy_kcal", unit="kcal"),
        NutrientSpec(source_field="energy-kj_100g", nutrient_key="energy_kj", unit="kJ"),
        # Some exports may only have energy_100g; OFF docs describe energy in kJ for _100g.
        NutrientSpec(source_field="energy_100g", nutrient_key="energy_kj", unit="kJ"),
        NutrientSpec(source_field="fat_100g", nutrient_key="fat", unit="g"),
        NutrientSpec(source_field="saturated-fat_100g", nutrient_key="saturated_fat", unit="g"),
        NutrientSpec(source_field="carbohydrates_100g", nutrient_key="carbohydrates", unit="g"),
        NutrientSpec(source_field="sugars_100g", nutrient_key="sugars", unit="g"),
        NutrientSpec(source_field="fiber_100g", nutrient_key="fiber", unit="g"),
        NutrientSpec(source_field="proteins_100g", nutrient_key="protein", unit="g"),
        NutrientSpec(source_field="sodium_100g", nutrient_key="sodium", unit="g"),
    ]
    if include_salt:
        specs.append(NutrientSpec(source_field="salt_100g", nutrient_key="salt", unit="g"))
    return specs


def normalize_nutrient_key_from_field(source_field: str) -> str:
    key = source_field
    if key.endswith("_100g"):
        key = key[: -len("_100g")]
    key = key.replace("-", "_").lower()
    return key


def unit_for_source_field(source_field: str) -> str:
    if source_field in {"energy-kj_100g", "energy_100g"}:
        return "kJ"
    if source_field == "energy-kcal_100g":
        return "kcal"
    return ""
