{{
    config(
        alias='sleep_daily',
        materialized='table',
        tags=['garmin', 'data4agent']
    )
}}

SELECT
    date AS sleep_date,
    sleep_calendar_date,
    sleep_overall_score,
    sleep_overall_score_qualifier,
    sleep_quality_key,
    sleep_score_feedback,
    sleep_time_seconds AS total_sleep_s,
    deep_sleep_seconds AS deep_sleep_s,
    light_sleep_seconds AS light_sleep_s,
    rem_sleep_seconds AS rem_sleep_s,
    awake_sleep_seconds AS awake_sleep_s,
    unmeasurable_sleep_seconds AS unmeasurable_sleep_s,
    nap_time_seconds AS nap_s,
    avgovernighthrv AS avg_overnight_hrv,
    hrvstatus AS hrv_status,
    restingheartrate AS resting_hr_bpm,
    bodybatterychange AS body_battery_change,
    sleep_avg_spo2_value AS avg_spo2,
    sleep_lowest_spo2_value AS lowest_spo2,
    sleep_avg_respiration AS avg_respiration_rpm,
    sleep_lowest_respiration AS lowest_respiration_rpm,
    sleep_highest_respiration AS highest_respiration_rpm,
    sleep_restless_moments_count AS restless_moments,
    sleep_start_timestamp_gmt AS sleep_start_gmt,
    sleep_end_timestamp_gmt AS sleep_end_gmt,
    sleep_start_timestamp_local AS sleep_start_local,
    sleep_end_timestamp_local AS sleep_end_local,
    _ingested_at
FROM {{ ref('dlk_garmin_svc__sleep') }}
WHERE date IS NOT NULL AND date >= '2025-01-01'
