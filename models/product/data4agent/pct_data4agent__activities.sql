{{
    config(
        alias='activities',
        materialized='table',
        tags=['garmin', 'data4agent']
    )
}}

SELECT  -- noqa: ST06
    a.activity_id,
    a.identity.activity_name,
    a.sport.type_key AS activity_type,
    DATE(TIMESTAMP(a.timing.start_at_local)) AS activity_date,
    TIMESTAMP(a.timing.start_at_local) AS start_time_local,
    a.timing.start_at_utc AS start_time_gmt,
    a.timing.end_at_utc AS end_time_gmt,
    a.identity.location_name,

    -- Distance & time
    a.performance.distance_m,
    SAFE_DIVIDE(a.performance.distance_m, 1000.0) AS distance_km,
    a.performance.duration_s,
    a.performance.moving_duration_s,
    a.performance.elapsed_duration_s,

    -- Pace & speed
    {{ speed_mps_to_pace_min_per_km('a.performance.avg_speed_m_per_s') }} AS avg_pace_min_per_km,
    a.performance.avg_speed_m_per_s,
    a.performance.max_speed_m_per_s,

    -- Elevation
    a.performance.elevation_gain_m,
    a.performance.elevation_loss_m,
    a.performance.min_elevation_m,
    a.performance.max_elevation_m,

    -- Heart rate
    a.performance.avg_hr_bpm,
    a.performance.max_hr_bpm,

    -- Cadence (running)
    a.running.avg_cadence_spm,
    a.running.max_cadence_spm,

    -- Power
    a.performance.avg_power_w,
    a.performance.max_power_w,
    a.performance.norm_power_w,

    -- Running dynamics
    a.running.avg_stride_length_m,
    a.running.avg_ground_contact_ms,
    a.running.avg_vertical_oscillation_mm,
    a.running.avg_vertical_ratio,

    -- Calories & training effect
    a.performance.calories,
    a.performance.bmr_calories,
    a.performance.aerobic_te,
    a.performance.anaerobic_te,
    a.performance.training_effect_label,
    a.performance.training_load,
    a.performance.vo2max_during_activity,

    -- Counts
    a.performance.steps,
    a.performance.lap_count,

    -- Flags
    a.flags.is_pr,
    a.flags.is_favorite,
    a.flags.is_manual,

    a._ingested_at
FROM {{ ref('hub_activities_svc__master_activities') }} AS a
WHERE
    TIMESTAMP(a.timing.start_at_local) IS NOT NULL
    AND DATE(TIMESTAMP(a.timing.start_at_local)) >= '2025-01-01'
ORDER BY TIMESTAMP(a.timing.start_at_local) DESC
