{{ config(materialized='view') }}

-- One heart rate summary per Garmin source profile and date. Summary values are reported source metrics; no sample-
-- weighted average is invented.
WITH normalized AS (
    SELECT
        s.date AS health_date,
        'profile' AS identity_scope,
        'garmin' AS source_system,
        'heart_rate' AS source_table,
        s._ingested_at,
        s.calendardate AS calendar_date,
        s.restingheartrate AS resting_hr_bpm,
        s.minheartrate AS min_hr_bpm,
        s.maxheartrate AS max_hr_bpm,
        s.lastsevendaysavgrestingheartrate AS resting_hr_7d_avg_bpm,
        s.starttimestampgmt AS start_at_utc,
        s.endtimestampgmt AS end_at_utc,
        CAST(s.userprofilepk AS STRING) AS source_user_id,
        ARRAY_LENGTH(s.heart_rate_values) AS sample_count,
        TO_JSON_STRING(STRUCT(s.date AS source_date, s.userprofilepk AS source_user_id)) AS source_record_key
    FROM {{ ref('dlk_garmin_svc__heart_rate') }} AS s
),

qualified AS (
    SELECT
        *,
        ARRAY_CONCAT(
            IF(health_date IS NULL, ['missing_health_date'], ARRAY<STRING>[]),
            IF(source_user_id IS NULL, ['missing_source_user_id'], ARRAY<STRING>[]),
            IF(resting_hr_bpm IS NOT NULL AND NOT (resting_hr_bpm > 0), ['invalid_resting_hr_bpm'], ARRAY<STRING>[]),
            IF(min_hr_bpm IS NOT NULL AND NOT (min_hr_bpm > 0), ['invalid_min_hr_bpm'], ARRAY<STRING>[]),
            IF(max_hr_bpm IS NOT NULL AND NOT (max_hr_bpm > 0), ['invalid_max_hr_bpm'], ARRAY<STRING>[]),
            IF(
                resting_hr_7d_avg_bpm IS NOT NULL AND NOT (resting_hr_7d_avg_bpm > 0),
                ['invalid_resting_hr_7d_avg_bpm'],
                ARRAY<STRING>[]
            ),
            IF(sample_count IS NOT NULL AND NOT (sample_count >= 0), ['invalid_sample_count'], ARRAY<STRING>[]),
            IF(max_hr_bpm < min_hr_bpm, ['invalid_hr_order'], ARRAY<STRING>[])
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
    calendar_date,
    start_at_utc,
    end_at_utc,
    _ingested_at,
    IF(resting_hr_bpm > 0, resting_hr_bpm, NULL) AS resting_hr_bpm,
    IF(min_hr_bpm > 0, min_hr_bpm, NULL) AS min_hr_bpm,
    IF(max_hr_bpm > 0, max_hr_bpm, NULL) AS max_hr_bpm,
    IF(resting_hr_7d_avg_bpm > 0, resting_hr_7d_avg_bpm, NULL) AS resting_hr_7d_avg_bpm,
    IF(sample_count >= 0, sample_count, NULL) AS sample_count,
    STRUCT(
        calendar_date,
        resting_hr_bpm,
        min_hr_bpm,
        max_hr_bpm,
        resting_hr_7d_avg_bpm,
        start_at_utc,
        end_at_utc,
        sample_count
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
