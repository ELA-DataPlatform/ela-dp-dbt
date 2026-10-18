{{ config(materialized='view') }}

-- One account-level daily Body Battery record, with extrema and endpoints derived from its sample series. Old epoch-
-- millisecond and new timestamp formats are normalized.
WITH battery_source AS (
    SELECT
        b.*,
        ARRAY(
            SELECT AS STRUCT
                {{ garmin_health_timestamp('v.timestamp_gmt') }} AS observed_at_utc,
                v.value AS level
            FROM UNNEST(b.body_battery_timeseries) AS v
            UNION ALL
            SELECT AS STRUCT
                SAFE.TIMESTAMP_MILLIS(v.timestamp) AS observed_at_utc,
                v.value AS level
            FROM UNNEST(b.body_battery_values) AS v
        ) AS health_battery_samples
    FROM {{ ref('dlk_garmin_svc__body_battery') }} AS b
),

normalized AS (
    SELECT
        s.date AS health_date,
        'account' AS identity_scope,
        'garmin' AS source_system,
        'body_battery' AS source_table,
        s._ingested_at,
        s.charged,
        s.drained,
        s.starttimestampgmt AS start_at_utc,
        s.endtimestampgmt AS end_at_utc,
        CAST(NULL AS STRING) AS source_user_id,
        (
            SELECT MIN(v.level) FROM UNNEST(s.health_battery_samples) AS v
            WHERE v.level BETWEEN 0 AND 100
        ) AS min_level,
        (
            SELECT MAX(v.level) FROM UNNEST(s.health_battery_samples) AS v
            WHERE v.level BETWEEN 0 AND 100
        ) AS max_level,
        (
            SELECT v.level
            FROM UNNEST(s.health_battery_samples) AS v
            WHERE v.level BETWEEN 0 AND 100 AND v.observed_at_utc IS NOT NULL
            ORDER BY v.observed_at_utc ASC, v.level ASC
            LIMIT 1
        ) AS first_level,
        (
            SELECT v.level
            FROM UNNEST(s.health_battery_samples) AS v
            WHERE v.level BETWEEN 0 AND 100 AND v.observed_at_utc IS NOT NULL
            ORDER BY v.observed_at_utc DESC, v.level ASC
            LIMIT 1
        ) AS last_level,
        ARRAY_LENGTH(s.health_battery_samples) AS sample_count,
        (
            SELECT COUNTIF(v.level IS NOT NULL AND (v.level NOT BETWEEN 0 AND 100 OR v.observed_at_utc IS NULL))
            FROM UNNEST(s.health_battery_samples) AS v
        ) AS invalid_sample_count,
        (SELECT COUNTIF(v.level IS NULL) FROM UNNEST(s.health_battery_samples) AS v) AS missing_sample_count,
        TO_JSON_STRING(STRUCT(s.date AS source_date, CAST(NULL AS STRING) AS source_user_id)) AS source_record_key
    FROM battery_source AS s
),

qualified AS (
    SELECT
        *,
        ARRAY_CONCAT(
            IF(health_date IS NULL, ['missing_health_date'], ARRAY<STRING>[]),
            IF(charged IS NOT NULL AND NOT (charged >= 0), ['invalid_charged'], ARRAY<STRING>[]),
            IF(drained IS NOT NULL AND NOT (drained >= 0), ['invalid_drained'], ARRAY<STRING>[]),
            IF(min_level IS NOT NULL AND NOT (min_level BETWEEN 0 AND 100), ['invalid_min_level'], ARRAY<STRING>[]),
            IF(max_level IS NOT NULL AND NOT (max_level BETWEEN 0 AND 100), ['invalid_max_level'], ARRAY<STRING>[]),
            IF(
                first_level IS NOT NULL AND NOT (first_level BETWEEN 0 AND 100),
                ['invalid_first_level'],
                ARRAY<STRING>[]
            ),
            IF(last_level IS NOT NULL AND NOT (last_level BETWEEN 0 AND 100), ['invalid_last_level'], ARRAY<STRING>[]),
            IF(sample_count IS NOT NULL AND NOT (sample_count >= 0), ['invalid_sample_count'], ARRAY<STRING>[]),
            IF(
                invalid_sample_count IS NOT NULL AND NOT (invalid_sample_count >= 0),
                ['invalid_invalid_sample_count'],
                ARRAY<STRING>[]
            ),
            IF(invalid_sample_count > 0, ['invalid_battery_samples'], ARRAY<STRING>[])
        ) AS _invalid_issues,
        ARRAY_CONCAT(
            IF(min_level IS NULL, ['missing_min_level'], ARRAY<STRING>[]),
            IF(max_level IS NULL, ['missing_max_level'], ARRAY<STRING>[]),
            IF(missing_sample_count > 0, ['missing_battery_samples'], ARRAY<STRING>[])
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
    start_at_utc,
    end_at_utc,
    _ingested_at,
    missing_sample_count,
    IF(charged >= 0, charged, NULL) AS charged,
    IF(drained >= 0, drained, NULL) AS drained,
    IF(min_level BETWEEN 0 AND 100, min_level, NULL) AS min_level,
    IF(max_level BETWEEN 0 AND 100, max_level, NULL) AS max_level,
    IF(first_level BETWEEN 0 AND 100, first_level, NULL) AS first_level,
    IF(last_level BETWEEN 0 AND 100, last_level, NULL) AS last_level,
    IF(sample_count >= 0, sample_count, NULL) AS sample_count,
    IF(invalid_sample_count >= 0, invalid_sample_count, NULL) AS invalid_sample_count,
    STRUCT(
        charged,
        drained,
        start_at_utc,
        end_at_utc,
        min_level,
        max_level,
        first_level,
        last_level,
        sample_count,
        invalid_sample_count,
        missing_sample_count
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
