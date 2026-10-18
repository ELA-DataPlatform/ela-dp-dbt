{{ config(materialized='view') }}

-- One training status per Garmin source user and date, using the source device selection. Numeric code is preserved;
-- unknown codes have no invented label.
WITH normalized AS (
    SELECT
        s.date AS health_date,
        'user' AS identity_scope,
        'garmin' AS source_system,
        'training_status' AS source_table,
        s._ingested_at,
        s.training_status AS status_code,
        s.training_status_feedback_phrase AS feedback_phrase,
        s.training_paused,
        s.sport,
        s.sub_sport,
        s.fitness_trend_sport,
        s.fitness_trend,
        s.acwr_status,
        s.acwr_status_feedback,
        s.acwr_percent,
        s.daily_acute_chronic_workload_ratio AS acwr_ratio,
        s.daily_training_load_acute AS acute_load,
        s.daily_training_load_chronic AS chronic_load,
        s.min_training_load_chronic AS chronic_load_lower_bound,
        s.max_training_load_chronic AS chronic_load_upper_bound,
        s.device_id,
        CAST(s.userid AS STRING) AS source_user_id,
        SAFE_CAST(s.calendar_date AS DATE) AS calendar_date,
        CASE s.training_status
            WHEN 0 THEN 'NO_STATUS' WHEN 1 THEN 'DETRAINING' WHEN 2 THEN 'RECOVERY' WHEN 3 THEN 'MAINTAINING' WHEN
                4
                THEN 'PRODUCTIVE'
            WHEN 5 THEN 'PEAKING' WHEN 6 THEN 'OVERREACHING' WHEN 7 THEN 'UNPRODUCTIVE' WHEN 8 THEN 'STRAINED'
        END AS status_label,
        SAFE_CAST(s.since_date AS DATE) AS since_date,
        TO_JSON_STRING(STRUCT(s.date AS source_date, s.userid AS source_user_id)) AS source_record_key
    FROM {{ ref('dlk_garmin_svc__training_status') }} AS s
),

qualified AS (
    SELECT
        *,
        ARRAY_CONCAT(
            IF(health_date IS NULL, ['missing_health_date'], ARRAY<STRING>[]),
            IF(source_user_id IS NULL, ['missing_source_user_id'], ARRAY<STRING>[]),
            IF(acwr_percent IS NOT NULL AND NOT (acwr_percent >= 0), ['invalid_acwr_percent'], ARRAY<STRING>[]),
            IF(acwr_ratio IS NOT NULL AND NOT (acwr_ratio >= 0), ['invalid_acwr_ratio'], ARRAY<STRING>[]),
            IF(acute_load IS NOT NULL AND NOT (acute_load >= 0), ['invalid_acute_load'], ARRAY<STRING>[]),
            IF(chronic_load IS NOT NULL AND NOT (chronic_load >= 0), ['invalid_chronic_load'], ARRAY<STRING>[]),
            IF(
                chronic_load_lower_bound IS NOT NULL AND NOT (chronic_load_lower_bound >= 0),
                ['invalid_chronic_load_lower_bound'],
                ARRAY<STRING>[]
            ),
            IF(
                chronic_load_upper_bound IS NOT NULL AND NOT (chronic_load_upper_bound >= 0),
                ['invalid_chronic_load_upper_bound'],
                ARRAY<STRING>[]
            )
        ) AS _invalid_issues,
        ARRAY_CONCAT(
            IF(status_code IS NULL, ['missing_status_code'], ARRAY<STRING>[])
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
    calendar_date,
    status_code,
    status_label,
    feedback_phrase,
    training_paused,
    sport,
    sub_sport,
    fitness_trend_sport,
    fitness_trend,
    acwr_status,
    acwr_status_feedback,
    since_date,
    device_id,
    _ingested_at,
    IF(acwr_percent >= 0, acwr_percent, NULL) AS acwr_percent,
    IF(acwr_ratio >= 0, acwr_ratio, NULL) AS acwr_ratio,
    IF(acute_load >= 0, acute_load, NULL) AS acute_load,
    IF(chronic_load >= 0, chronic_load, NULL) AS chronic_load,
    IF(chronic_load_lower_bound >= 0, chronic_load_lower_bound, NULL) AS chronic_load_lower_bound,
    IF(chronic_load_upper_bound >= 0, chronic_load_upper_bound, NULL) AS chronic_load_upper_bound,
    STRUCT(
        calendar_date,
        status_code,
        status_label,
        feedback_phrase,
        training_paused,
        sport,
        sub_sport,
        fitness_trend_sport,
        fitness_trend,
        acwr_status,
        acwr_status_feedback,
        acwr_percent,
        acwr_ratio,
        acute_load,
        chronic_load,
        chronic_load_lower_bound,
        chronic_load_upper_bound,
        since_date,
        device_id
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
