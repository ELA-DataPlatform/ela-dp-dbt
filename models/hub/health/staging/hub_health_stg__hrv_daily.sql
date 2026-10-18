{{ config(materialized='view') }}

-- One HRV summary per Garmin source profile and local date. All HRV measurements use milliseconds.
WITH normalized AS (
    SELECT
        s.hrv_date AS health_date,
        'profile' AS identity_scope,
        'garmin' AS source_system,
        'hrv' AS source_table,
        s._ingested_at,
        s.hrv_calendar_date,
        s.hrv_status,
        s.hrv_feedback_phrase,
        s.hrv_last_night_avg,
        s.hrv_last_night_5min_high,
        s.hrv_weekly_avg,
        s.hrv_baseline_low_upper,
        s.hrv_baseline_balanced_low,
        s.hrv_baseline_balanced_upper,
        s.hrv_baseline_marker_value,
        CAST(s.userprofilepk AS STRING) AS source_user_id,
        TO_JSON_STRING(STRUCT(s.hrv_date AS source_date, s.userprofilepk AS source_user_id)) AS source_record_key
    FROM {{ ref('dlk_garmin_svc__hrv') }} AS s
),

qualified AS (
    SELECT
        *,
        ARRAY_CONCAT(
            IF(health_date IS NULL, ['missing_health_date'], ARRAY<STRING>[]),
            IF(source_user_id IS NULL, ['missing_source_user_id'], ARRAY<STRING>[]),
            IF(
                hrv_last_night_avg IS NOT NULL AND NOT (hrv_last_night_avg > 0),
                ['invalid_hrv_last_night_avg'],
                ARRAY<STRING>[]
            ),
            IF(
                hrv_last_night_5min_high IS NOT NULL AND NOT (hrv_last_night_5min_high > 0),
                ['invalid_hrv_last_night_5min_high'],
                ARRAY<STRING>[]
            ),
            IF(hrv_weekly_avg IS NOT NULL AND NOT (hrv_weekly_avg > 0), ['invalid_hrv_weekly_avg'], ARRAY<STRING>[]),
            IF(
                hrv_baseline_low_upper IS NOT NULL AND NOT (hrv_baseline_low_upper > 0),
                ['invalid_hrv_baseline_low_upper'],
                ARRAY<STRING>[]
            ),
            IF(
                hrv_baseline_balanced_low IS NOT NULL AND NOT (hrv_baseline_balanced_low > 0),
                ['invalid_hrv_baseline_balanced_low'],
                ARRAY<STRING>[]
            ),
            IF(
                hrv_baseline_balanced_upper IS NOT NULL AND NOT (hrv_baseline_balanced_upper > 0),
                ['invalid_hrv_baseline_balanced_upper'],
                ARRAY<STRING>[]
            ),
            IF(
                hrv_baseline_marker_value IS NOT NULL AND NOT (hrv_baseline_marker_value > 0),
                ['invalid_hrv_baseline_marker_value'],
                ARRAY<STRING>[]
            )
        ) AS _invalid_issues,
        ARRAY_CONCAT(
            IF(hrv_last_night_avg IS NULL, ['missing_hrv_last_night_avg'], ARRAY<STRING>[])
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
    hrv_calendar_date,
    hrv_status,
    hrv_feedback_phrase,
    _ingested_at,
    IF(hrv_last_night_avg > 0, hrv_last_night_avg, NULL) AS hrv_last_night_avg,
    IF(hrv_last_night_5min_high > 0, hrv_last_night_5min_high, NULL) AS hrv_last_night_5min_high,
    IF(hrv_weekly_avg > 0, hrv_weekly_avg, NULL) AS hrv_weekly_avg,
    IF(hrv_baseline_low_upper > 0, hrv_baseline_low_upper, NULL) AS hrv_baseline_low_upper,
    IF(hrv_baseline_balanced_low > 0, hrv_baseline_balanced_low, NULL) AS hrv_baseline_balanced_low,
    IF(hrv_baseline_balanced_upper > 0, hrv_baseline_balanced_upper, NULL) AS hrv_baseline_balanced_upper,
    IF(hrv_baseline_marker_value > 0, hrv_baseline_marker_value, NULL) AS hrv_baseline_marker_value,
    STRUCT(
        hrv_calendar_date,
        hrv_status,
        hrv_feedback_phrase,
        hrv_last_night_avg,
        hrv_last_night_5min_high,
        hrv_weekly_avg,
        hrv_baseline_low_upper,
        hrv_baseline_balanced_low,
        hrv_baseline_balanced_upper,
        hrv_baseline_marker_value
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
