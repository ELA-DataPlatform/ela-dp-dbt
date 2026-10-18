{{ config(materialized='view') }}

-- One resting heart rate summary per Garmin source profile and local date, in beats per minute.
WITH normalized AS (
    SELECT
        s.date AS health_date,
        'profile' AS identity_scope,
        'garmin' AS source_system,
        'rhr_daily' AS source_table,
        s._ingested_at,
        s.resting_heart_rate AS resting_hr_bpm,
        CAST(s.userprofileid AS STRING) AS source_user_id,
        SAFE_CAST(s.rhr_calendar_date AS DATE) AS rhr_calendar_date,
        TO_JSON_STRING(STRUCT(s.date AS source_date, s.userprofileid AS source_user_id)) AS source_record_key
    FROM {{ ref('dlk_garmin_svc__rhr_daily') }} AS s
),

qualified AS (
    SELECT
        *,
        ARRAY_CONCAT(
            IF(health_date IS NULL, ['missing_health_date'], ARRAY<STRING>[]),
            IF(source_user_id IS NULL, ['missing_source_user_id'], ARRAY<STRING>[]),
            IF(resting_hr_bpm IS NOT NULL AND NOT (resting_hr_bpm > 0), ['invalid_resting_hr_bpm'], ARRAY<STRING>[])
        ) AS _invalid_issues,
        ARRAY_CONCAT(
            IF(resting_hr_bpm IS NULL, ['missing_resting_hr_bpm'], ARRAY<STRING>[])
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
    rhr_calendar_date,
    _ingested_at,
    IF(resting_hr_bpm > 0, resting_hr_bpm, NULL) AS resting_hr_bpm,
    STRUCT(rhr_calendar_date, resting_hr_bpm) AS source_values,
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
