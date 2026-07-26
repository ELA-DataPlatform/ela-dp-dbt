{{
    config(
        materialized='incremental',
        unique_key='activity_id',
        incremental_strategy='merge',
        tags=['garmin', 'webapp']
    )
}}

SELECT
    hub.activity_id,
    hub.activity_name,
    hub.activity_type,
    hub.activity_date,
    hub.start_time_local,
    hub.end_time_gmt,
    hub.location_name,
    hub.is_pr,
    hub.is_favorite,

    hub.route,

    hub.weather.temp_celsius,
    hub.weather.apparent_temp_celsius,
    hub.weather.humidity_pct,
    hub.weather.wind_speed_kph,
    hub.weather.wind_direction_deg,
    hub.weather.wind_direction_compass,
    hub.weather.weather_type,

    {{ meters_to_kilometers('hub.performance.distance_m', 2) }} AS distance_km,
    CAST(ROUND(hub.performance.duration_s) AS INT64) AS duration_seconds,
    {{ format_duration_label('hub.performance.duration_s') }} AS duration_label,

    hub.performance.avg_pace_min_per_km,
    CAST(
        {{ minutes_per_km_to_seconds_per_km('hub.performance.avg_pace_min_per_km') }}
        AS INT64
    ) AS avg_pace_seconds_per_km,
    CAST(
        ROUND(
            {{ speed_mps_to_pace_min_per_km(
                'hub.performance.avg_grade_adjusted_speed_m_per_s'
            ) }} * 60
        )
        AS INT64
    ) AS avg_gap_seconds_per_km,
    CASE
        WHEN
            hub.performance.avg_pace_min_per_km IS NOT NULL
            AND hub.performance.avg_pace_min_per_km > 0
            THEN {{ format_pace_label('hub.performance.avg_pace_min_per_km') }}
    END AS pace_label,

    CAST(ROUND(hub.performance.avg_hr_bpm) AS INT64) AS avg_hr_bpm,
    CAST(ROUND(hub.performance.max_hr_bpm) AS INT64) AS max_hr_bpm,
    ROUND(
        SAFE_DIVIDE(hub.performance.avg_hr_bpm, hub.performance.max_hr_bpm) * 100,
        1
    ) AS avg_hr_pct_of_activity_max,
    CAST(ROUND(hub.performance.elevation_gain_m) AS INT64) AS elevation_gain_m,
    CAST(ROUND(hub.performance.elevation_loss_m) AS INT64) AS elevation_loss_m,
    ROUND(hub.performance.avg_cadence_spm, 1) AS avg_cadence_spm,
    ROUND(hub.performance.training_load, 1) AS training_load,
    ROUND(hub.performance.aerobic_te, 1) AS aerobic_training_effect,
    ROUND(hub.performance.anaerobic_te, 1) AS anaerobic_training_effect,
    hub.performance.training_effect_label,
    hub.performance.aerobic_te_message,
    hub.performance.anaerobic_te_message,

    CAST(ROUND(hub.performance.calories) AS INT64) AS calories,
    {{ speed_mps_to_kmh('hub.performance.avg_speed_m_per_s') }} AS avg_speed_km_h,
    CAST(ROUND(hub.performance.steps) AS INT64) AS steps,
    CAST(ROUND(hub.performance.lap_count) AS INT64) AS lap_count,
    hub.performance.vo2max_during_activity,

    hub.fastest_splits,

    hub.athletic_context.readiness_score,
    hub.athletic_context.readiness_level,
    hub.athletic_context.vo2_max,
    hub.athletic_context.weight_kg,
    hub.athletic_context.training_status_code,
    hub.athletic_context.training_status_feedback,
    hub.athletic_context.acute_training_load,
    hub.athletic_context.recovery_time_hours,

    hub.sleep.overall_score AS sleep_score,
    hub.sleep.score_qualifier AS sleep_score_qualifier,
    hub.sleep.total_sleep_s,
    hub.sleep.avg_overnight_hrv AS sleep_hrv,

    hub._ingested_at

FROM {{ ref('svc_hub__master_running_activities') }} AS hub

{% if is_incremental() %}
    WHERE hub._ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{% endif %}
