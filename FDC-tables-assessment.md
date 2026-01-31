This is an assessment of the USDA FoodData Central “Full Download” CSV bundle
`usda/FoodData_Central_csv_2025-12-18/`, categorized by utility for a consumer-facing
nutrition app (including “nerdy” features).

Important: the Oct 2020 “Data Dictionary” PDF is out of sync with the 2025 CSV release.
This document is based on the **actual CSV filenames + headers** in the 2025-12-18 bundle
(i.e., no guessing about what files exist or how they join).

## The 4 datasets (how they show up in CSVs)

In the full download, the “datasets” (Foundation, Branded, SR Legacy, FNDDS Survey) are not
separate databases. They are primarily distinguished by `food.csv.data_type`, and then
some datasets add a 1:1 “extension table” keyed by the same `fdc_id`.

Legend used below:
- **F** = Foundation Foods
- **B** = Branded Foods
- **SR** = Standard Reference (SR Legacy)
- **FN** = FNDDS (Survey Foods)

`Size*` is the raw CSV file size from `usda/FoodData_Central_csv_2025-12-18/`.

## Already imported

In schema `usda` we’ve imported:

- `branded_food.csv`
- `fndds_derivation.csv`
- `fndds_ingredient_nutrient_value.csv`
- `food.csv`
- `food_calorie_conversion_factor.csv`
- `food_category.csv`
- `food_component.csv`
- `food_nutrient.csv`
- `food_nutrient_conversion_factor.csv`
- `food_nutrient_derivation.csv`
- `food_nutrient_source.csv`
- `food_portion.csv`
- `food_protein_conversion_factor.csv`
- `foundation_food.csv`
- `input_food.csv`
- `measure_unit.csv`
- `microbe.csv`
- `nutrient.csv`
- `retention_factor.csv`
- `sr_legacy_food.csv`
- `survey_fndds_food.csv`
- `wweia_food_category.csv`

### 1. High Value / Interest (The "Must Haves")

These tables are required for the four “high level datasets” to be useful in an app
(Foundation, Branded, Standard Reference (SR Legacy), FNDDS Survey Foods).

| Table | Size* | Supports | Relates via | Why it is critical |
| --- | --- | --- | --- | --- |
| **`nutrient.csv`** | 21K | F / B / SR / FN | `food_nutrient.nutrient_id → nutrient.id` | **Essential.** Defines nutrient IDs, names, and units. Without this, you can’t label nutrients. |
| **`food_nutrient.csv`** | 1.7G | F / B / SR / FN | `food_nutrient.fdc_id → food.fdc_id` | **Essential.** Nutrient measurements (massive). Without this, you can’t show nutrition. |
| **`measure_unit.csv`** | 1.9K | F / B / SR / FN | `food_portion.measure_unit_id → measure_unit.id` | **Essential for portion UX.** Without this, portions show up as unknown unit IDs. |
| **`food_portion.csv`** | 3.2M | F / B / SR / FN | `food_portion.fdc_id → food.fdc_id` | **Essential for portion UX.** Enables household serving display and gram-weight conversions; without it you’re mostly stuck with per-100g views. |

Notes:
- Foundation / SR Legacy / FNDDS / Branded all share `food.csv` + `food_nutrient.csv` + `nutrient.csv` as the common backbone.
- The importer already loads `food.csv`; `food_nutrient.csv` is the single biggest “core” table by volume.

---

### 2. Possible Interest for "Nerdy" Consumers

Given your interest in thermodynamics and "deep" data, these tables provide the "Why" behind the numbers. They explain how specific calorie counts were derived (it's not always just 4-4-9) and what generic ingredients make up a composite meal.

| Table | Size* | Supports | Relates via | Why a "Nerd" might care |
| --- | --- | --- | --- | --- |
| **`food_nutrient_conversion_factor.csv`** | 13K | F / B / SR / FN | `food_nutrient_conversion_factor.fdc_id → food.fdc_id` | Connects a food to a conversion-factor “bundle” ID, used by the two tables below. |
| **`food_calorie_conversion_factor.csv`** | 141K | F / B / SR / FN | `food_calorie_conversion_factor.food_nutrient_conversion_factor_id → food_nutrient_conversion_factor.id` | Calorie Atwater factors per food (via conversion-factor bundle). Enables “true calorie math” views. |
| **`food_protein_conversion_factor.csv`** | 5.4K | F / B / SR / FN | `food_protein_conversion_factor.food_nutrient_conversion_factor_id → food_nutrient_conversion_factor.id` | Protein conversion factor per food (via conversion-factor bundle). Useful for “nitrogen-to-protein” precision. |
| **`food_nutrient_derivation.csv`** | 8.1K | F / B / SR / FN | `food_nutrient.derivation_id → food_nutrient_derivation.id` | Decodes how a nutrient value was derived (“calculated vs analyzed” type views). |
| **`food_nutrient_source.csv`** | 602B | F / B / SR / FN | (referenced by some releases; not present in the core `food_nutrient.csv` header in this bundle) | Decodes nutrient source codes; provenance/quality metadata. |
| **`survey_fndds_food.csv`** | 287K | FN | `survey_fndds_food.fdc_id → food.fdc_id` | Adds survey-specific fields (e.g. `food_code`, `wweia_category_code`, date range). Useful if you want “FNDDS as a dataset”, not just nutrients. |
| **`wweia_food_category.csv`** | 5.0K | FN | `survey_fndds_food.wweia_category_code → wweia_food_category.wweia_food_category` | Better categorization for FNDDS browsing/filtering. |
| **`input_food.csv`** | 1.9M | FN | `input_food.fdc_id → food.fdc_id` and `input_food.fdc_id_of_input_food → food.fdc_id` | For survey foods: deconstructs a composite food into ingredient foods + gram weights; enables “explode a meal into components”. |
| **`fndds_ingredient_nutrient_value.csv`** | 35M | FN | Has `FDC ID` and “ingredient code” / “nutrient code”; requires careful normalization | Ingredient/nutrient mapping used by FNDDS derivations. Column naming is not normalized; needs careful schema handling. |
| **`fndds_derivation.csv`** | 4.7K | FN | “derivation code” dictionary for FNDDS tables | Decodes derivation codes used in FNDDS mapping rows. |
| **`retention_factor.csv`** | 13K | FN | `input_food.retention_code → retention_factor.n.code` (no `fdc_id`) | Cooking retention reference data; useful for a “cooking effects” feature, not required for basic nutrition display. |
| **`foundation_food.csv`** | 16K | F | `foundation_food.fdc_id → food.fdc_id` | Adds Foundation-only metadata (`NDB_number`, `footnote`). Usually low consumer-facing value, but can occasionally contain user-visible clarifications; can help later for validation/crosswalks; **tiny (cheap RU/storage)**. |
| **`sr_legacy_food.csv`** | 127K | SR | `sr_legacy_food.fdc_id → food.fdc_id` | Adds SR-Legacy-only metadata (`NDB_number`). Mostly useful for crosswalks/validation; **tiny (cheap RU/storage)**. |

---

### 3. Ignore / Don't Bother (Backend Noise)

These tables are artifacts of the USDA's data collection logistics. They are useful for agricultural researchers auditing the USDA's work, but they are noise for your application.

| Table | Size* | Supports | Relates via | Why you should ignore it |
| --- | --- | --- | --- | --- |
| **`food_update_log_entry.csv`** | 105M | F / B / SR / FN | (standalone) | Operational change log; not required for nutrition lookups. |
| **`market_acquisition.csv`** | 869K | F (sampling) | `market_acquisition.fdc_id` | USDA sampling logistics (store/city/upc/acquisition metadata). |
| **`acquisition_samples.csv`** | 148K | F (sampling) | `fdc_id_of_sample_food`, `fdc_id_of_acquisition_food` | Sampling join table; not needed unless auditing sampling. |
| **`agricultural_samples.csv`** | 37K | F (sampling) | `agricultural_samples.fdc_id` | Sampling provenance for agricultural items. |
| **`sample_food.csv`** | 36K | F (sampling) | `sample_food.fdc_id` | Sampling provenance key for Foundation sampling. |
| **`sub_sample_food.csv`** | 1.2M | F (sampling) | `sub_sample_food.fdc_id`, `fdc_id_of_sample_food` | Sampling provenance linking subsamples to sample foods. |
| **`sub_sample_result.csv`** | 4.6M | F (sampling) | `sub_sample_result.food_nutrient_id → food_nutrient.id` and `sub_sample_result.lab_method_id → lab_method.id` | Lab-method-specific adjustments for particular nutrient measurements. |
| **`lab_method.csv`** | 12K | F (sampling) | `lab_method.id` | Lab technique dictionary (chemistry/audit focus). |
| **`lab_method_code.csv`** | 3.9K | F (sampling) | `lab_method_code.lab_method_id → lab_method.id` | Helper mapping lab method to a code. |
| **`lab_method_nutrient.csv`** | 7.8K | F (sampling) | `lab_method_nutrient.lab_method_id → lab_method.id`, `lab_method_nutrient.nutrient_id → nutrient.id` | Helper mapping lab method to nutrients. |
| **`all_downloaded_table_record_counts.csv`** | 1010B | (download metadata) | (standalone) | Packaging metadata for the download; not needed for app queries (and not always reliable). |
| **`food_attribute.csv`** | 131M | F / B / SR / FN | `food_attribute.fdc_id → food.fdc_id` | High complexity for low payoff: attributes are sparse/inconsistent across foods and often redundant with `food.description` / `branded_food` text; hard to build reliable UI filters. |
| **`food_attribute_type.csv`** | 331B | F / B / SR / FN | `food_attribute.food_attribute_type_id → food_attribute_type.id` | Dictionary for `food_attribute.csv` (only needed if importing `food_attribute.csv`). |

Niche (we imported these, but they are not core):
- `food_component.csv`: component breakdown by `fdc_id`.
- `microbe.csv`: uses `foodId` (not `fdc_id`); may require special handling/mapping to be useful.

### Summary of Actionable Schema

Current state:
- Imported all non-ignore tables listed in Sections 1–2 (plus the “Niche” items above).
- Not imported: the “Ignore / Don’t Bother” tables in Section 3.
