This is an assessment of the USDA FoodData Central “Full Download” CSV bundle
`usda/FoodData_Central_csv_2025-12-18/`, categorized by utility for a consumer-facing
nutrition app (including “nerdy” features).

Important: the Oct 2020 “Data Dictionary” PDF is out of sync with the 2025 CSV release.
This document is based on the **actual CSV filenames + headers** in the 2025-12-18 bundle
(i.e., no guessing about what files exist or how they join).

## Already imported

- `food_category.csv`
- `food.csv`
- `branded_food.csv`

### 1. High Value / Interest (The "Must Haves")

These are the remaining tables that are required for the four “high level datasets” to be
useful in an app (Foundation, Branded, Standard Reference (SR Legacy), FNDDS Survey Foods).

| Table | Why it is critical |
| --- | --- |
| **`nutrient.csv`** | **Essential.** Defines nutrient IDs, names, and units. `food_nutrient.csv` references `nutrient_id`. Without this, you can’t label nutrients. |
| **`food_nutrient.csv`** | **Essential.** This is the nutrient measurements table (massive). It links `fdc_id` (from `food.csv`) to `nutrient_id` (from `nutrient.csv`) and provides the `amount`. Without this, you can’t show nutrition. |
| **`measure_unit.csv`** | **Essential for portions.** `food_portion.csv` refers to units by ID (e.g., ID = “cup”). Without this, portion display is broken/opaque. |
| **`food_portion.csv`** | **Essential for UX.** Provides gram weights for household portions (“1 cup”, “1 slice”, etc.) via `measure_unit_id`. Without this you’re mostly stuck with per-100g views. |

Notes:
- Foundation / SR Legacy / FNDDS / Branded all share `food.csv` + `food_nutrient.csv` + `nutrient.csv` as the common backbone.
- The importer already loads `food.csv`; importing `food_nutrient.csv` is the single biggest remaining “core” step.

---

### 2. Possible Interest for "Nerdy" Consumers

Given your interest in thermodynamics and "deep" data, these tables provide the "Why" behind the numbers. They explain how specific calorie counts were derived (it's not always just 4-4-9) and what generic ingredients make up a composite meal.

| Table | Why a "Nerd" might care |
| --- | --- |
| **`food_nutrient_conversion_factor.csv`** | Links conversion factors to `fdc_id` (via `id`, `fdc_id`). Used by the two tables below. |
| **`food_calorie_conversion_factor.csv`** | Calorie Atwater factors keyed by `food_nutrient_conversion_factor_id` (join through `food_nutrient_conversion_factor.csv` to reach a specific `fdc_id`). Enables “true calorie math” views. |
| **`food_protein_conversion_factor.csv`** | Protein conversion factor keyed by `food_nutrient_conversion_factor_id` (same join path as above). Useful for “nitrogen-to-protein” precision. |
| **`food_nutrient_derivation.csv`** | Defines derivation codes (how a nutrient value was determined). Useful for provenance/quality and for nerdy UI (“calculated vs analyzed”). |
| **`food_nutrient_source.csv`** | Defines nutrient source codes. Useful for provenance/quality. |
| **`input_food.csv`** | **FNDDS (Survey Foods) only.** Describes how a composite survey food is built from input foods (`fdc_id_of_input_food`) with gram weights and retention codes. Enables “explode a meal into components”. |
| **`fndds_ingredient_nutrient_value.csv`** | **FNDDS only.** Ingredient/nutrient mapping and values used in FNDDS derivations. Column naming is not normalized; needs careful schema handling. |
| **`fndds_derivation.csv`** | **FNDDS only.** Decodes “derivation code” values found in FNDDS nutrient-value rows. |
| **`retention_factor.csv`** | Cooking retention reference table (not keyed by `fdc_id`). Useful for a “cooking effects” feature, but not required for basic nutrition display. |
| **`wweia_food_category.csv`** | **FNDDS browsing.** Maps `wweia_category_code` (from `survey_fndds_food.csv`) to human-readable categories; improves browsing/filtering. |
| **`food_attribute.csv`** | Per-food attributes by `fdc_id` (with a type table). Can add UI facets (tags/flags/extra descriptors) depending on how populated it is in the release. |
| **`food_attribute_type.csv`** | Type dictionary for `food_attribute.csv`. |

---

### 3. Ignore / Don't Bother (Backend Noise)

These tables are artifacts of the USDA's data collection logistics. They are useful for agricultural researchers auditing the USDA's work, but they are noise for your application.

| Table | Why you should ignore it |
| --- | --- |
| **`food_update_log_entry.csv`** | Operational change log; not required for nutrition lookups. |
| **`market_acquisition.csv`** | USDA sampling logistics (store/city/upc/acquisition metadata). |
| **`acquisition_samples.csv`** | Sampling join table linking sample foods to acquisition foods; not needed unless you’re auditing sampling. |
| **`agricultural_samples.csv`** | Sampling provenance for agricultural items. |
| **`sample_food.csv`** | Sampling provenance key (`fdc_id`) for Foundation sampling. |
| **`sub_sample_food.csv`** | Sampling provenance linking subsamples to sample foods. |
| **`sub_sample_result.csv`** | Lab-method-specific adjustments for particular nutrient measurements. |
| **`lab_method.csv`** | Lab technique dictionary (chemistry/audit focus). |
| **`lab_method_code.csv`** | Helper mapping lab method to a code. |
| **`lab_method_nutrient.csv`** | Helper mapping lab method to nutrients. |
| **`all_downloaded_table_record_counts.csv`** | Packaging metadata for the download; not needed for app queries (and not always reliable). |

“It depends” / niche:
- `food_component.csv`: component breakdown by `fdc_id` (could be useful, but not core).
- `microbe.csv`: has `foodId` (not `fdc_id`); may require special handling/mapping to be useful.

### Summary of Actionable Schema

To build the “core” version of your app across Foundation + Branded + SR Legacy + FNDDS:

1. **Already Imported:** `food.csv`, `food_category.csv`, `branded_food.csv`.
2. **Must Import Next:** `nutrient.csv`, `food_nutrient.csv`.
3. **Strongly Recommended for UX:** `measure_unit.csv`, `food_portion.csv`.
4. **Optional “Pro” Features:** `wweia_food_category.csv`, `input_food.csv`, `fndds_ingredient_nutrient_value.csv`, `food_calorie_conversion_factor.csv` (+ `food_nutrient_conversion_factor.csv`), `food_protein_conversion_factor.csv`.
