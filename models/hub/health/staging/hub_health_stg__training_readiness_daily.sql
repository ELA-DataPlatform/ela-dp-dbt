{{ config(materialized='view') }}

-- One latest available daily readiness snapshot per Garmin source profile. The bronze already collapses intraday
-- snapshots; this model cannot recover them. Recovery time is minutes, with an explicit hours conversion and
-- REACHED_ZERO handling.
WITH normalized AS (
    SELECT
        s.calendardate AS health_date,
        'profile' AS identity_scope,
        'garmin' AS source_system,
        'training_readiness' AS source_table,
        s._ingested_at,
        s.timestamp AS observed_at_utc,
        s.deviceid AS device_id,
        s.score AS readiness_score,
        s.level AS readiness_level,
        s.feedbackshort AS feedback_short,
        s.inputcontext AS input_context,
        s.recoverytimechangephrase AS recovery_time_change_phrase,
        s.recoverytime AS source_recovery_time_min,
        s.sleepscorefactorpercent AS sleep_factor_pct,
        s.hrvfactorpercent AS hrv_factor_pct,
        s.stresshistoryfactorpercent AS stress_history_factor_pct,
        s.recoverytimefactorpercent AS recovery_time_factor_pct,
        s.acwrfactorpercent AS acwr_factor_pct,
        s.hrvweeklyaverage AS hrv_weekly_average_ms,
        s.acuteload AS acute_load,
        CAST(s.userprofilepk AS STRING) AS source_user_id,
        DATETIME(s.timestamplocal) AS observed_at_local,
        IF(s.recoverytimechangephrase = 'REACHED_ZERO', 0, s.recoverytime) AS recovery_time_min,
        SAFE_DIVIDE(IF(s.recoverytimechangephrase = 'REACHED_ZERO', 0, s.recoverytime), 60.0) AS recovery_time_hours,
        TO_JSON_STRING(STRUCT(s.calendardate AS source_date, s.userprofilepk AS source_user_id)) AS source_record_key
    FROM {{ ref('dlk_garmin_svc__training_readiness') }} AS s
),

qualified AS (
    SELECT
        *,
        ARRAY_CONCAT(
            IF(health_date IS NULL, ['missing_health_date'], ARRAY<STRING>[]),
            IF(source_user_id IS NULL, ['missing_source_user_id'], ARRAY<STRING>[]),
            IF(
                readiness_score IS NOT NULL AND NOT (readiness_score BETWEEN 0 AND 100),
                ['invalid_readiness_score'],
                ARRAY<STRING>[]
            ),
            IF(
                source_recovery_time_min IS NOT NULL AND NOT (source_recovery_time_min >= 0),
                ['invalid_source_recovery_time_min'],
                ARRAY<STRING>[]
            ),
            IF(
                recovery_time_min IS NOT NULL AND NOT (recovery_time_min >= 0),
                ['invalid_recovery_time_min'],
                ARRAY<STRING>[]
            ),
            IF(
                recovery_time_hours IS NOT NULL AND NOT (recovery_time_hours >= 0),
                ['invalid_recovery_time_hours'],
                ARRAY<STRING>[]
            ),
            IF(
                sleep_factor_pct IS NOT NULL AND NOT (sleep_factor_pct BETWEEN 0 AND 100),
                ['invalid_sleep_factor_pct'],
                ARRAY<STRING>[]
            ),
            IF(
                hrv_factor_pct IS NOT NULL AND NOT (hrv_factor_pct BETWEEN 0 AND 100),
                ['invalid_hrv_factor_pct'],
                ARRAY<STRING>[]
            ),
            IF(
                stress_history_factor_pct IS NOT NULL AND NOT (stress_history_factor_pct BETWEEN 0 AND 100),
                ['invalid_stress_history_factor_pct'],
                ARRAY<STRING>[]
            ),
            IF(
                recovery_time_factor_pct IS NOT NULL AND NOT (recovery_time_factor_pct BETWEEN 0 AND 100),
                ['invalid_recovery_time_factor_pct'],
                ARRAY<STRING>[]
            ),
            IF(
                acwr_factor_pct IS NOT NULL AND NOT (acwr_factor_pct BETWEEN 0 AND 100),
                ['invalid_acwr_factor_pct'],
                ARRAY<STRING>[]
            ),
            IF(
                hrv_weekly_average_ms IS NOT NULL AND NOT (hrv_weekly_average_ms > 0),
                ['invalid_hrv_weekly_average_ms'],
                ARRAY<STRING>[]
            ),
            IF(acute_load IS NOT NULL AND NOT (acute_load >= 0), ['invalid_acute_load'], ARRAY<STRING>[])
        ) AS _invalid_issues,
        ARRAY_CONCAT(
            IF(readiness_score IS NULL, ['missing_readiness_score'], ARRAY<STRING>[])
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
    observed_at_utc,
    observed_at_local,
    device_id,
    readiness_level,
    feedback_short,
    input_context,
    recovery_time_change_phrase,
    _ingested_at,
    IF(readiness_score BETWEEN 0 AND 100, readiness_score, NULL) AS readiness_score,
    IF(source_recovery_time_min >= 0, source_recovery_time_min, NULL) AS source_recovery_time_min,
    IF(recovery_time_min >= 0, recovery_time_min, NULL) AS recovery_time_min,
    IF(recovery_time_hours >= 0, recovery_time_hours, NULL) AS recovery_time_hours,
    IF(sleep_factor_pct BETWEEN 0 AND 100, sleep_factor_pct, NULL) AS sleep_factor_pct,
    IF(hrv_factor_pct BETWEEN 0 AND 100, hrv_factor_pct, NULL) AS hrv_factor_pct,
    IF(stress_history_factor_pct BETWEEN 0 AND 100, stress_history_factor_pct, NULL) AS stress_history_factor_pct,
    IF(recovery_time_factor_pct BETWEEN 0 AND 100, recovery_time_factor_pct, NULL) AS recovery_time_factor_pct,
    IF(acwr_factor_pct BETWEEN 0 AND 100, acwr_factor_pct, NULL) AS acwr_factor_pct,
    IF(hrv_weekly_average_ms > 0, hrv_weekly_average_ms, NULL) AS hrv_weekly_average_ms,
    IF(acute_load >= 0, acute_load, NULL) AS acute_load,
    STRUCT(
        observed_at_utc,
        observed_at_local,
        device_id,
        readiness_score,
        readiness_level,
        feedback_short,
        input_context,
        recovery_time_change_phrase,
        source_recovery_time_min,
        recovery_time_min,
        recovery_time_hours,
        sleep_factor_pct,
        hrv_factor_pct,
        stress_history_factor_pct,
        recovery_time_factor_pct,
        acwr_factor_pct,
        hrv_weekly_average_ms,
        acute_load
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
    ORDER BY observed_at_utc DESC, _ingested_at DESC, TO_JSON_STRING(q) ASC
) = 1
