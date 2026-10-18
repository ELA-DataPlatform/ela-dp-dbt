{{
    config(
        alias='sleep_daily',
        materialized='table',
        tags=['garmin', 'data4agent']
    )
}}

SELECT
    health_date AS sleep_date,
    sleep_score AS sleep_overall_score,
    score_qualifier AS sleep_overall_score_qualifier,
    quality_key AS sleep_quality_key,
    score_feedback AS sleep_score_feedback,
    total_sleep_s,
    deep_sleep_s,
    light_sleep_s,
    rem_sleep_s,
    awake_sleep_s,
    unmeasurable_sleep_s,
    nap_s,
    avg_overnight_hrv_ms AS avg_overnight_hrv,
    hrv_status,
    resting_hr_bpm,
    body_battery_change,
    avg_spo2_pct AS avg_spo2,
    lowest_spo2_pct AS lowest_spo2,
    avg_respiration_rpm,
    lowest_respiration_rpm,
    highest_respiration_rpm,
    restless_moments,
    source_values.sleep_start_source_value AS sleep_start_gmt,
    source_values.sleep_end_source_value AS sleep_end_gmt,
    source_values.sleep_start_local_source_value AS sleep_start_local,
    source_values.sleep_end_local_source_value AS sleep_end_local,
    _ingested_at,
    CAST(sleep_calendar_date AS STRING) AS sleep_calendar_date
FROM {{ ref('hub_health_svc__sleep_daily') }}
WHERE health_date IS NOT NULL AND health_date >= '2025-01-01'
