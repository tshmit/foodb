from __future__ import annotations


def ddl(*, schema: str) -> list[str]:
    return [
        f"CREATE SCHEMA IF NOT EXISTS {schema}",
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.product_raw (
            code_norm STRING NOT NULL,
            code_raw STRING,
            product_name STRING,
            brands STRING,
            categories STRING,
            quantity STRING,
            serving_size STRING,
            last_modified_t INT8,
            energy_kcal_100g FLOAT8,
            energy_kj_100g FLOAT8,
            fat_100g FLOAT8,
            saturated_fat_100g FLOAT8,
            carbohydrates_100g FLOAT8,
            sugars_100g FLOAT8,
            fiber_100g FLOAT8,
            protein_100g FLOAT8,
            sodium_100g FLOAT8,
            salt_100g FLOAT8,
            PRIMARY KEY (code_norm)
        )
        """.strip(),
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.nutrient_100g (
            code_norm STRING NOT NULL,
            nutrient_key STRING NOT NULL,
            value FLOAT8,
            unit STRING,
            source_field STRING,
            PRIMARY KEY (code_norm, nutrient_key)
        )
        """.strip(),
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.import_metadata (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            source_url STRING,
            file_path STRING,
            file_sha256 STRING,
            file_bytes INT8,
            delimiter STRING,
            nutrients_mode STRING
        )
        """.strip(),
    ]
