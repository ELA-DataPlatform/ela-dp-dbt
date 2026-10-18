{{ config(materialized='view') }}

-- One oxygen saturation summary per Garmin source profile and date. Percentages are in (0,100]; absent or invalid
-- observations stay missing, never zero-filled. Tomorrow-night values are excluded.
WITH normalized AS (
    SELECT
        s.date AS health_date,
        'profile' AS identity_scope,
        'garmin' AS source_system,
        'spo2' AS source_table,
        s._ingested_at,
        s.calendardate AS calendar_date,
        s.averagespo2 AS avg_spo2_pct,
        s.lowestspo2 AS lowest_spo2_pct,
        s.avgsleepspo2 AS avg_sleep_spo2_pct,
        s.latestspo2 AS latest_spo2_pct,
        s.latestspo2timestampgmt AS latest_reading_at_utc,
        s.deviceid AS device_id,
        CAST(s.userprofilepk AS STRING) AS source_user_id,
        ARRAY_LENGTH(s.spo2_values) AS sample_count,
        TO_JSON_STRING(STRUCT(s.date AS source_date, s.userprofilepk AS source_user_id)) AS source_record_key
    FROM {{ ref('dlk_garmin_svc__spo2') }} AS s
),

qualified AS (
    SELECT
        *,
        ARRAY_CONCAT(
            IF(health_date IS NULL, ['missing_health_date'], ARRAY<STRING>[]),
            IF(source_user_id IS NULL, ['missing_source_user_id'], ARRAY<STRING>[]),
            IF(
                avg_spo2_pct IS NOT NULL AND NOT (avg_spo2_pct > 0 AND avg_spo2_pct <= 100),
                ['invalid_avg_spo2_pct'],
                ARRAY<STRING>[]
            ),
            IF(
                lowest_spo2_pct IS NOT NULL AND NOT (lowest_spo2_pct > 0 AND lowest_spo2_pct <= 100),
                ['invalid_lowest_spo2_pct'],
                ARRAY<STRING>[]
            ),
            IF(
                avg_sleep_spo2_pct IS NOT NULL AND NOT (avg_sleep_spo2_pct > 0 AND avg_sleep_spo2_pct <= 100),
                ['invalid_avg_sleep_spo2_pct'],
                ARRAY<STRING>[]
            ),
            IF(
                latest_spo2_pct IS NOT NULL AND NOT (latest_spo2_pct > 0 AND latest_spo2_pct <= 100),
                ['invalid_latest_spo2_pct'],
                ARRAY<STRING>[]
            ),
            IF(sample_count IS NOT NULL AND NOT (sample_count >= 0), ['invalid_sample_count'], ARRAY<STRING>[])
        ) AS _invalid_issues,
        ARRAY_CONCAT(
            IF(avg_spo2_pct IS NULL, ['missing_avg_spo2_pct'], ARRAY<STRING>[])
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
    latest_reading_at_utc,
    device_id,
    _ingested_at,
    IF(avg_spo2_pct > 0 AND avg_spo2_pct <= 100, avg_spo2_pct, NULL) AS avg_spo2_pct,
    IF(lowest_spo2_pct > 0 AND lowest_spo2_pct <= 100, lowest_spo2_pct, NULL) AS lowest_spo2_pct,
    IF(avg_sleep_spo2_pct > 0 AND avg_sleep_spo2_pct <= 100, avg_sleep_spo2_pct, NULL) AS avg_sleep_spo2_pct,
    IF(latest_spo2_pct > 0 AND latest_spo2_pct <= 100, latest_spo2_pct, NULL) AS latest_spo2_pct,
    IF(sample_count >= 0, sample_count, NULL) AS sample_count,
    STRUCT(
        calendar_date,
        avg_spo2_pct,
        lowest_spo2_pct,
        avg_sleep_spo2_pct,
        latest_spo2_pct,
        latest_reading_at_utc,
        device_id,
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
