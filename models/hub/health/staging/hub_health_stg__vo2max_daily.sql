{{ config(materialized='view') }}

-- One generic VO2max observation per Garmin source user and date, in ml/kg/min. No forward-fill, and cycling payload
-- is deliberately excluded.
WITH normalized AS (
    SELECT
        s.calendar_date AS health_date,
        'user' AS identity_scope,
        'garmin' AS source_system,
        'max_metrics' AS source_table,
        s._ingested_at,
        s.vo2_max,
        s.vo2_max_precise,
        s.max_met_category,
        s.heat_acclimation_pct,
        s.altitude_acclimation AS altitude_acclimation_source_value,
        s.current_altitude AS current_altitude_source_value,
        CAST(s.userid AS STRING) AS source_user_id,
        SAFE_CAST(s.vo2_max_calendar_date AS DATE) AS measurement_date,
        TO_JSON_STRING(STRUCT(s.calendar_date AS source_date, s.userid AS source_user_id)) AS source_record_key
    FROM {{ ref('dlk_garmin_svc__max_metrics') }} AS s
),

qualified AS (
    SELECT
        *,
        ARRAY_CONCAT(
            IF(health_date IS NULL, ['missing_health_date'], ARRAY<STRING>[]),
            IF(source_user_id IS NULL, ['missing_source_user_id'], ARRAY<STRING>[]),
            IF(vo2_max IS NOT NULL AND NOT (vo2_max > 0), ['invalid_vo2_max'], ARRAY<STRING>[]),
            IF(vo2_max_precise IS NOT NULL AND NOT (vo2_max_precise > 0), ['invalid_vo2_max_precise'], ARRAY<STRING>[]),
            IF(
                heat_acclimation_pct IS NOT NULL AND NOT (heat_acclimation_pct BETWEEN 0 AND 100),
                ['invalid_heat_acclimation_pct'],
                ARRAY<STRING>[]
            )
        ) AS _invalid_issues,
        ARRAY_CONCAT(
            IF(vo2_max IS NULL AND vo2_max_precise IS NULL, ['missing_vo2_max'], ARRAY<STRING>[])
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
    measurement_date,
    max_met_category,
    altitude_acclimation_source_value,
    current_altitude_source_value,
    _ingested_at,
    IF(vo2_max > 0, vo2_max, NULL) AS vo2_max,
    IF(vo2_max_precise > 0, vo2_max_precise, NULL) AS vo2_max_precise,
    IF(heat_acclimation_pct BETWEEN 0 AND 100, heat_acclimation_pct, NULL) AS heat_acclimation_pct,
    STRUCT(
        vo2_max,
        vo2_max_precise,
        measurement_date,
        max_met_category,
        heat_acclimation_pct,
        altitude_acclimation_source_value,
        current_altitude_source_value
    ) AS source_values,
    CASE
        WHEN ARRAY_LENGTH(_invalid_issues) > 0 THEN 'invalid' WHEN
            ARRAY_LENGTH(_missing_issues) > 0
            THEN 'partial'
        ELSE 'valid'
    END AS quality_status,
    ARRAY_CONCAT(_invalid_issues, _missing_issues) AS quality_issues
FROM qualified AS q
QUALIFY health_date IS NULL OR ROW_NUMBER() OVER (
    PARTITION BY source_user_id, health_date
    ORDER BY _ingested_at DESC, TO_JSON_STRING(q) ASC
) = 1
