{{
    config(
        alias='activities',
        materialized='table',
        tags=['garmin'],
        partition_by={'field': 'activity_date', 'data_type': 'date', 'granularity': 'month'},
        cluster_by=['activity_type']
    )
}}

SELECT  -- noqa: ST06
    a.activity_id,
    a.identity.activity_name,
    a.sport.type_key AS activity_type,
    a.activity_date,
    a.timing.start_at_local,
    a.timing.start_at_utc,
    a.timing.end_at_utc,
    a.identity.location_name,

    -- Distance & time
    a.performance.distance_m,
    {{ meters_to_kilometers('a.performance.distance_m') }} AS distance_km,
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
    a.running.avg_vertical_ratio AS avg_vertical_ratio_pct,

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

    a.quality_status,
    a.quality_issues,
    a.availability,
    a.provenance,
    a._ingested_at,
    a._silver_processed_at AS _hub_processed_at,
    'Europe/Paris' AS timezone,
    CURRENT_TIMESTAMP() AS _product_built_at
FROM {{ ref('hub_activities_svc__master_activities') }} AS a
