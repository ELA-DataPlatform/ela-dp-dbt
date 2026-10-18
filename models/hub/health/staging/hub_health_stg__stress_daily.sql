{{ config(materialized='view') }}

-- One daily stress summary per Garmin source profile. Negative no-data sentinels are exposed in source_values and
-- removed from qualified scores.
WITH normalized AS (
    SELECT
        s.date AS health_date,
        'profile' AS identity_scope,
        'garmin' AS source_system,
        'stress' AS source_table,
        s._ingested_at,
        s.calendardate AS calendar_date,
        s.avgstresslevel AS avg_stress_level,
        s.maxstresslevel AS max_stress_level,
        s.starttimestampgmt AS start_at_utc,
        s.endtimestampgmt AS end_at_utc,
        CAST(s.userprofilepk AS STRING) AS source_user_id,
        DATETIME(s.starttimestamplocal) AS start_at_local,
        DATETIME(s.endtimestamplocal) AS end_at_local,
        TO_JSON_STRING(STRUCT(s.date AS source_date, s.userprofilepk AS source_user_id)) AS source_record_key
    FROM {{ ref('dlk_garmin_svc__stress') }} AS s
),

qualified AS (
    SELECT
        *,
        ARRAY_CONCAT(
            IF(health_date IS NULL, ['missing_health_date'], ARRAY<STRING>[]),
            IF(source_user_id IS NULL, ['missing_source_user_id'], ARRAY<STRING>[]),
            IF(
                avg_stress_level IS NOT NULL AND NOT (avg_stress_level BETWEEN 0 AND 100),
                ['invalid_avg_stress_level'],
                ARRAY<STRING>[]
            ),
            IF(
                max_stress_level IS NOT NULL AND NOT (max_stress_level BETWEEN 0 AND 100),
                ['invalid_max_stress_level'],
                ARRAY<STRING>[]
            ),
            IF(end_at_utc < start_at_utc, ['invalid_measurement_window'], ARRAY<STRING>[])
        ) AS _invalid_issues,
        ARRAY_CONCAT(
            IF(avg_stress_level IS NULL, ['missing_avg_stress_level'], ARRAY<STRING>[])
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
    start_at_local,
    end_at_local,
    _ingested_at,
    IF(avg_stress_level BETWEEN 0 AND 100, avg_stress_level, NULL) AS avg_stress_level,
    IF(max_stress_level BETWEEN 0 AND 100, max_stress_level, NULL) AS max_stress_level,
    STRUCT(calendar_date, avg_stress_level, max_stress_level, start_at_utc, end_at_utc, start_at_local, end_at_local)
        AS source_values,
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
