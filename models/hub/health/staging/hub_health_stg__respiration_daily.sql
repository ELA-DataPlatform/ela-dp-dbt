{{ config(materialized='view') }}

-- One respiration summary per Garmin source profile and local date, in breaths per minute. Tomorrow-night fields are
-- excluded to avoid mixing future context.
WITH normalized AS (
    SELECT
        s.date AS health_date,
        'profile' AS identity_scope,
        'garmin' AS source_system,
        'respiration' AS source_table,
        s._ingested_at,
        s.calendardate AS calendar_date,
        s.avgwakingrespirationvalue AS avg_awake_respiration_rpm,
        s.avgsleeprespirationvalue AS avg_sleep_respiration_rpm,
        s.lowestrespirationvalue AS min_respiration_rpm,
        s.highestrespirationvalue AS max_respiration_rpm,
        s.sleepstarttimestampgmt AS sleep_start_at_utc,
        s.sleependtimestampgmt AS sleep_end_at_utc,
        CAST(s.userprofilepk AS STRING) AS source_user_id,
        ARRAY_LENGTH(s.respiration_values) AS sample_count,
        TO_JSON_STRING(STRUCT(s.date AS source_date, s.userprofilepk AS source_user_id)) AS source_record_key
    FROM {{ ref('dlk_garmin_svc__respiration') }} AS s
),

qualified AS (
    SELECT
        *,
        ARRAY_CONCAT(
            IF(health_date IS NULL, ['missing_health_date'], ARRAY<STRING>[]),
            IF(source_user_id IS NULL, ['missing_source_user_id'], ARRAY<STRING>[]),
            IF(
                avg_awake_respiration_rpm IS NOT NULL AND NOT (avg_awake_respiration_rpm > 0),
                ['invalid_avg_awake_respiration_rpm'],
                ARRAY<STRING>[]
            ),
            IF(
                avg_sleep_respiration_rpm IS NOT NULL AND NOT (avg_sleep_respiration_rpm > 0),
                ['invalid_avg_sleep_respiration_rpm'],
                ARRAY<STRING>[]
            ),
            IF(
                min_respiration_rpm IS NOT NULL AND NOT (min_respiration_rpm > 0),
                ['invalid_min_respiration_rpm'],
                ARRAY<STRING>[]
            ),
            IF(
                max_respiration_rpm IS NOT NULL AND NOT (max_respiration_rpm > 0),
                ['invalid_max_respiration_rpm'],
                ARRAY<STRING>[]
            ),
            IF(sample_count IS NOT NULL AND NOT (sample_count >= 0), ['invalid_sample_count'], ARRAY<STRING>[]),
            IF(max_respiration_rpm < min_respiration_rpm, ['invalid_respiration_order'], ARRAY<STRING>[])
        ) AS _invalid_issues,
        ARRAY_CONCAT(
            IF(avg_awake_respiration_rpm IS NULL, ['missing_avg_awake_respiration_rpm'], ARRAY<STRING>[])
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
    sleep_start_at_utc,
    sleep_end_at_utc,
    _ingested_at,
    IF(avg_awake_respiration_rpm > 0, avg_awake_respiration_rpm, NULL) AS avg_awake_respiration_rpm,
    IF(avg_sleep_respiration_rpm > 0, avg_sleep_respiration_rpm, NULL) AS avg_sleep_respiration_rpm,
    IF(min_respiration_rpm > 0, min_respiration_rpm, NULL) AS min_respiration_rpm,
    IF(max_respiration_rpm > 0, max_respiration_rpm, NULL) AS max_respiration_rpm,
    IF(sample_count >= 0, sample_count, NULL) AS sample_count,
    STRUCT(
        calendar_date,
        avg_awake_respiration_rpm,
        avg_sleep_respiration_rpm,
        min_respiration_rpm,
        max_respiration_rpm,
        sleep_start_at_utc,
        sleep_end_at_utc,
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
