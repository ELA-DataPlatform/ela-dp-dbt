{{ config(materialized='view') }}

-- One account-level body measurement per sample ID, falling back to measurement timestamp and date when no sample ID
-- exists. Reconciles weight and body_composition; richer composition values can fill nullable fields.
WITH measurement_observations AS (
    SELECT
        w.samplepk,
        w.timestampgmt,
        w.calendardate,
        w.sourcetype,
        w.weight,
        w.bodyfat,
        w.bodywater,
        w.bmi,
        w.musclemass,
        w.bonemass,
        w.metabolicage,
        w.visceralfat,
        w._ingested_at,
        'weight' AS source_table,
        1 AS source_priority
    FROM {{ ref('dlk_garmin_svc__weight') }} AS w
    WHERE NOT (w.samplepk IS NULL AND w.timestampgmt IS NULL AND w.weight IS NULL)

    UNION ALL

    SELECT
        b.samplepk,
        b.timestampgmt,
        b.calendardate,
        b.sourcetype,
        b.weight,
        b.bodyfat,
        b.bodywater,
        b.bmi,
        b.musclemass,
        b.bonemass,
        b.metabolicage,
        b.visceralfat,
        b._ingested_at,
        'body_composition' AS source_table,
        0 AS source_priority
    FROM {{ ref('dlk_garmin_svc__body_composition') }} AS b
    WHERE NOT (b.samplepk IS NULL AND b.timestampgmt IS NULL AND b.weight IS NULL)
),

measurement_keys AS (
    SELECT
        *,
        COALESCE(
            CAST(samplepk AS STRING),
            CONCAT('timestamp:', CAST(timestampgmt AS STRING), ':', CAST(calendardate AS STRING))
        ) AS measurement_key
    FROM measurement_observations
),

measurement_source AS (
    SELECT
        s.* EXCEPT (bodyfat, bodywater, bmi, musclemass, bonemass, metabolicage, visceralfat, _ingested_at),
        MAX(s._ingested_at) OVER (
            PARTITION BY measurement_key, IF(measurement_key IS NULL, TO_JSON_STRING(s), NULL)
        ) AS _ingested_at,
        FIRST_VALUE(bodyfat IGNORE NULLS) OVER measurement_window AS bodyfat,
        FIRST_VALUE(bodywater IGNORE NULLS) OVER measurement_window AS bodywater,
        FIRST_VALUE(bmi IGNORE NULLS) OVER measurement_window AS bmi,
        FIRST_VALUE(musclemass IGNORE NULLS) OVER measurement_window AS musclemass,
        FIRST_VALUE(bonemass IGNORE NULLS) OVER measurement_window AS bonemass,
        FIRST_VALUE(metabolicage IGNORE NULLS) OVER measurement_window AS metabolicage,
        FIRST_VALUE(visceralfat IGNORE NULLS) OVER measurement_window AS visceralfat
    FROM measurement_keys AS s
    QUALIFY
        measurement_key IS NULL OR ROW_NUMBER() OVER (
            PARTITION BY measurement_key
            ORDER BY s._ingested_at DESC, source_priority ASC, TO_JSON_STRING(s) ASC
        ) = 1
    WINDOW measurement_window AS (
        PARTITION BY measurement_key, IF(measurement_key IS NULL, TO_JSON_STRING(s), NULL)
        ORDER BY s._ingested_at DESC, source_priority ASC, TO_JSON_STRING(s) ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
),

normalized AS (
    SELECT
        s.calendardate AS health_date,
        CAST(NULL AS STRING) AS source_user_id,
        'account' AS identity_scope,
        'garmin' AS source_system,
        s.source_table,
        s._ingested_at,
        s.samplepk AS sample_id,
        {{ garmin_health_timestamp('s.timestampgmt') }} AS measured_at_utc,
        s.sourcetype AS measurement_source_type,
        SAFE_DIVIDE(s.weight, 1000.0) AS weight_kg,
        s.bodyfat AS body_fat_pct,
        s.bodywater AS body_water_pct,
        s.bmi,
        SAFE_DIVIDE(s.musclemass, 1000.0) AS muscle_mass_kg,
        SAFE_DIVIDE(s.bonemass, 1000.0) AS bone_mass_kg,
        s.metabolicage AS metabolic_age_source_value,
        s.visceralfat AS visceral_fat_source_value,
        s.measurement_key,
        TO_JSON_STRING(
            STRUCT(s.calendardate AS source_date, CAST(NULL AS STRING) AS source_user_id, s.measurement_key)
        ) AS source_record_key
    FROM measurement_source AS s
),

qualified AS (
    SELECT
        *,
        ARRAY_CONCAT(
            IF(health_date IS NULL, ['missing_health_date'], ARRAY<STRING>[]),
            IF(measurement_key IS NULL, ['missing_measurement_key'], ARRAY<STRING>[]),
            IF(weight_kg IS NOT NULL AND NOT (weight_kg > 0), ['invalid_weight_kg'], ARRAY<STRING>[]),
            IF(
                body_fat_pct IS NOT NULL AND NOT (body_fat_pct BETWEEN 0 AND 100),
                ['invalid_body_fat_pct'],
                ARRAY<STRING>[]
            ),
            IF(
                body_water_pct IS NOT NULL AND NOT (body_water_pct BETWEEN 0 AND 100),
                ['invalid_body_water_pct'],
                ARRAY<STRING>[]
            ),
            IF(bmi IS NOT NULL AND NOT (bmi > 0), ['invalid_bmi'], ARRAY<STRING>[]),
            IF(muscle_mass_kg IS NOT NULL AND NOT (muscle_mass_kg >= 0), ['invalid_muscle_mass_kg'], ARRAY<STRING>[]),
            IF(bone_mass_kg IS NOT NULL AND NOT (bone_mass_kg >= 0), ['invalid_bone_mass_kg'], ARRAY<STRING>[]),
            IF(measured_at_utc IS NULL, ['invalid_measurement_timestamp'], ARRAY<STRING>[])
        ) AS _invalid_issues,
        ARRAY_CONCAT(
            IF(weight_kg IS NULL, ['missing_weight_kg'], ARRAY<STRING>[])
        ) AS _missing_issues
    FROM normalized
)

SELECT
    health_date,
    source_user_id,
    identity_scope,
    source_system,
    source_table,
    source_record_key,
    sample_id,
    measured_at_utc,
    measurement_source_type,
    metabolic_age_source_value,
    visceral_fat_source_value,
    measurement_key,
    _ingested_at,
    IF(weight_kg > 0, weight_kg, NULL) AS weight_kg,
    IF(body_fat_pct BETWEEN 0 AND 100, body_fat_pct, NULL) AS body_fat_pct,
    IF(body_water_pct BETWEEN 0 AND 100, body_water_pct, NULL) AS body_water_pct,
    IF(bmi > 0, bmi, NULL) AS bmi,
    IF(muscle_mass_kg >= 0, muscle_mass_kg, NULL) AS muscle_mass_kg,
    IF(bone_mass_kg >= 0, bone_mass_kg, NULL) AS bone_mass_kg,
    STRUCT(
        sample_id,
        measured_at_utc,
        measurement_source_type,
        weight_kg,
        body_fat_pct,
        body_water_pct,
        bmi,
        muscle_mass_kg,
        bone_mass_kg,
        metabolic_age_source_value,
        visceral_fat_source_value
    ) AS source_values,
    CASE
        WHEN ARRAY_LENGTH(_invalid_issues) > 0 THEN 'invalid' WHEN
            ARRAY_LENGTH(_missing_issues) > 0
            THEN 'partial'
        ELSE 'valid'
    END AS quality_status,
    ARRAY_CONCAT(_invalid_issues, _missing_issues) AS quality_issues
FROM qualified AS q
QUALIFY health_date IS NULL OR measurement_key IS NULL OR ROW_NUMBER() OVER (
    PARTITION BY measurement_key
    ORDER BY _ingested_at DESC, TO_JSON_STRING(q) ASC
) = 1
