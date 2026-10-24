{{ config(materialized='view') }}

-- Descriptive context at local calendar-day grain, for the current platform account.
-- Profile/user identifiers are NOT asserted to identify the activity owner.
WITH
sleep AS (
    SELECT
        _ingested_at AS source_ingested_at,
        quality_status AS source_quality_status,
        quality_issues AS source_quality_issues,
        health_date,
        sleep_score AS sleep_overall_score,
        score_qualifier AS sleep_overall_score_qualifier,
        quality_key AS sleep_quality_key,
        total_sleep_s AS sleep_time_seconds,
        deep_sleep_s AS deep_sleep_seconds,
        light_sleep_s AS light_sleep_seconds,
        rem_sleep_s AS rem_sleep_seconds,
        awake_sleep_s AS awake_sleep_seconds,
        avg_overnight_hrv_ms AS avgovernighthrv,
        hrv_status AS hrvstatus,
        resting_hr_bpm AS restingheartrate,
        body_battery_change AS bodybatterychange,
        avg_spo2_pct AS sleep_avg_spo2_value,
        lowest_spo2_pct AS sleep_lowest_spo2_value,
        avg_respiration_rpm AS sleep_avg_respiration,
        score_feedback AS sleep_score_feedback,
        source_values.sleep_start_source_value AS sleep_start_timestamp_gmt,
        source_values.sleep_end_source_value AS sleep_end_timestamp_gmt
    FROM {{ ref('hub_health_svc__sleep_daily') }}
    QUALIFY COUNT(*) OVER (PARTITION BY health_date) = 1
),

-- ── 7. Training readiness ────────────────────────────────────────────────────
training_readiness AS (
    SELECT
        _ingested_at AS source_ingested_at,
        quality_status AS source_quality_status,
        quality_issues AS source_quality_issues,
        health_date AS calendardate,
        readiness_score AS score,
        readiness_level AS level,
        feedback_short AS feedbackshort,
        sleep_factor_pct AS sleepscorefactorpercent,
        hrv_factor_pct AS hrvfactorpercent,
        stress_history_factor_pct AS stresshistoryfactorpercent,
        recovery_time_factor_pct AS recoverytimefactorpercent,
        acwr_factor_pct AS acwrfactorpercent,
        acute_load AS acuteload,
        recovery_time_hours,
        hrv_weekly_average_ms AS hrvweeklyaverage
    FROM {{ ref('hub_health_svc__training_readiness_daily') }}
    QUALIFY COUNT(*) OVER (PARTITION BY health_date) = 1
),

-- ── 8. VO2max: most recent qualified observation valid on the activity date ────────────
max_metrics AS (
    SELECT
        _ingested_at AS source_ingested_at,
        quality_status AS source_quality_status,
        quality_issues AS source_quality_issues,
        health_date AS calendar_date,
        vo2_max,
        vo2_max_precise,
        CAST(measurement_date AS STRING) AS vo2_max_calendar_date,
        LEAD(health_date) OVER (
            ORDER BY health_date
        ) AS valid_until
    FROM {{ ref('hub_health_svc__vo2max_daily') }}
    WHERE COALESCE(vo2_max_precise, vo2_max) IS NOT NULL
),

-- ── 9. Weight: most recent qualified observation valid on the activity date ────────────
weight_daily AS (
    SELECT *
    FROM {{ ref('hub_health_svc__body_measurement') }}
    WHERE weight_kg IS NOT NULL AND measured_at_utc IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY health_date
        ORDER BY measured_at_utc DESC, _ingested_at DESC, measurement_key ASC
    ) = 1
),

weight_measurements AS (
    SELECT
        health_date AS calendardate,
        _ingested_at,
        quality_status,
        quality_issues,
        weight_kg,
        body_fat_pct AS bodyfat,
        bmi,
        muscle_mass_kg,
        bone_mass_kg,
        LEAD(health_date) OVER (
            ORDER BY health_date
        ) AS valid_until
    FROM weight_daily
),

-- ── 10. Training status ──────────────────────────────────────────────────────
training_status AS (
    SELECT
        _ingested_at AS source_ingested_at,
        quality_status AS source_quality_status,
        quality_issues AS source_quality_issues,
        health_date,
        status_code AS code,
        feedback_phrase,
        fitness_trend,
        acwr_status,
        acwr_percent,
        acute_load AS daily_load_acute,
        acwr_ratio AS daily_acwr_ratio
    FROM {{ ref('hub_health_svc__training_status_daily') }}
    QUALIFY COUNT(*) OVER (PARTITION BY health_date) = 1
)

SELECT
    a.activity_id,
    STRUCT(
        s.health_date AS sleep_date,
        s.sleep_overall_score AS overall_score,
        s.sleep_overall_score_qualifier AS score_qualifier,
        s.sleep_quality_key AS quality_key,
        s.sleep_time_seconds AS total_sleep_s,
        s.deep_sleep_seconds AS deep_s,
        s.light_sleep_seconds AS light_s,
        s.rem_sleep_seconds AS rem_s,
        s.awake_sleep_seconds AS awake_s,
        s.avgovernighthrv AS avg_overnight_hrv,
        s.hrvstatus AS hrv_status,
        s.restingheartrate AS resting_hr_bpm,
        s.bodybatterychange AS body_battery_change,
        s.sleep_avg_spo2_value AS avg_spo2,
        s.sleep_lowest_spo2_value AS lowest_spo2,
        s.sleep_avg_respiration AS avg_respiration_rpm,
        s.sleep_score_feedback AS score_feedback,
        s.sleep_start_timestamp_gmt AS start_gmt,
        s.sleep_end_timestamp_gmt AS end_gmt
    ) AS sleep,
    STRUCT(
        -- Training readiness (null when Garmin doesn't provide it for this date)
        tr.score AS readiness_score,
        tr.level AS readiness_level,
        tr.feedbackshort AS readiness_feedback,
        tr.sleepscorefactorpercent AS readiness_sleep_factor_pct,
        tr.hrvfactorpercent AS readiness_hrv_factor_pct,
        tr.stresshistoryfactorpercent AS readiness_stress_factor_pct,
        tr.recoverytimefactorpercent AS readiness_recovery_factor_pct,
        tr.acwrfactorpercent AS readiness_acwr_factor_pct,
        tr.acuteload AS acute_training_load,
        tr.recovery_time_hours,
        tr.hrvweeklyaverage AS hrv_weekly_average,
        -- Training status
        ts.code AS training_status_code,
        ts.feedback_phrase AS training_status_feedback,
        ts.fitness_trend,
        ts.acwr_status,
        ts.acwr_percent,
        ts.daily_load_acute AS ts_acute_load,
        ts.daily_acwr_ratio AS ts_acwr_ratio,
        -- VO2max (most recent valid measurement)
        mm.vo2_max,
        mm.vo2_max_precise,
        mm.vo2_max_calendar_date AS vo2max_date,
        -- Body composition (most recent valid measurement)
        wm.weight_kg,
        wm.bodyfat AS body_fat_pct,
        wm.bmi,
        CAST(ROUND(wm.muscle_mass_kg * 1000.0) AS INT64) AS muscle_mass_g,
        CAST(ROUND(wm.bone_mass_kg * 1000.0) AS INT64) AS bone_mass_g
    ) AS athletic_context,
    ARRAY(
        SELECT AS STRUCT p.*
        FROM
            UNNEST([
                STRUCT(
                    'sleep_daily' AS source_table, s.source_ingested_at AS source_ingested_at,
                    s.source_quality_status AS quality_status, s.source_quality_issues AS quality_issues
                ),
                STRUCT(
                    'training_readiness_daily',
                    tr.source_ingested_at,
                    tr.source_quality_status,
                    tr.source_quality_issues
                ),
                STRUCT(
                    'training_status_daily', ts.source_ingested_at, ts.source_quality_status, ts.source_quality_issues
                ),
                STRUCT('vo2max_daily', mm.source_ingested_at, mm.source_quality_status, mm.source_quality_issues),
                STRUCT('body_measurement', wm._ingested_at, wm.quality_status, wm.quality_issues)
            ]) AS p
        WHERE p.source_ingested_at IS NOT NULL
        ORDER BY p.source_table
    ) AS provenance,
    STRUCT(
        'platform_account_local_date' AS method,
        FALSE AS identity_match_guaranteed,
        FALSE AS known_before_start_guaranteed,
        'descriptive_daily' AS temporal_scope,
        s.health_date AS sleep_date,
        tr.calendardate AS readiness_date,
        ts.health_date AS training_status_date,
        mm.calendar_date AS vo2max_context_date,
        wm.calendardate AS body_measurement_date
    ) AS association
FROM {{ ref('hub_activities_svc__fact_activity') }} AS a
LEFT JOIN sleep AS s ON a.activity_date = s.health_date
LEFT JOIN training_readiness AS tr ON a.activity_date = tr.calendardate
LEFT JOIN training_status AS ts ON a.activity_date = ts.health_date
LEFT JOIN max_metrics AS mm
    ON a.activity_date >= mm.calendar_date AND (mm.valid_until IS NULL OR a.activity_date < mm.valid_until)
LEFT JOIN weight_measurements AS wm
    ON a.activity_date >= wm.calendardate AND (wm.valid_until IS NULL OR a.activity_date < wm.valid_until)
