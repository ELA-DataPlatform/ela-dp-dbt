{{
    config(
        alias='activity_intervals',
        materialized='table',
        tags=['garmin'],
        partition_by={'field': 'activity_date', 'data_type': 'date', 'granularity': 'month'},
        cluster_by=['activity_type', 'activity_id']
    )
}}

SELECT  -- noqa: ST06
    lap.item_key AS interval_id,
    lap.source_index,
    s.activity_id,
    a.identity.activity_name,
    a.sport.type_key AS activity_type,
    a.activity_date,
    lap.lap_index,
    lap.message_index,
    lap.intensity_type,

    -- Time
    lap.start_at_utc,
    lap.duration_s,
    lap.moving_duration_s,
    lap.elapsed_duration_s,

    -- Distance & speed
    lap.distance_m,
    {{ meters_to_kilometers('lap.distance_m') }} AS distance_km,
    lap.avg_speed_m_per_s,
    lap.avg_moving_speed_m_per_s,
    lap.max_speed_m_per_s,
    {{ speed_mps_to_pace_min_per_km('lap.avg_speed_m_per_s') }} AS avg_pace_min_per_km,
    lap.avg_grade_adjusted_speed_m_per_s,

    -- Elevation
    lap.elevation_gain_m,
    lap.elevation_loss_m,
    lap.min_elevation_m,
    lap.max_elevation_m,
    lap.max_vertical_speed_m_per_s,

    -- Heart rate
    lap.avg_hr_bpm,
    lap.max_hr_bpm,

    -- Cadence
    lap.avg_run_cadence_spm AS avg_cadence_spm,
    lap.max_run_cadence_spm AS max_cadence_spm,

    -- Power
    lap.avg_power_w,
    lap.max_power_w,
    lap.min_power_w,
    lap.norm_power_w,
    lap.total_work_kj,

    -- Running dynamics
    lap.stride_length_m AS avg_stride_length_m,
    lap.ground_contact_ms AS avg_ground_contact_ms,
    lap.vertical_oscillation_mm AS avg_vertical_oscillation_mm,
    lap.vertical_ratio_pct AS avg_vertical_ratio_pct,

    -- Calories & temperature
    lap.calories,
    lap.bmr_calories,
    lap.avg_temp_celsius,
    lap.min_temp_celsius,
    lap.max_temp_celsius,

    -- GPS
    lap.start_latitude,
    lap.start_longitude,
    lap.end_latitude,
    lap.end_longitude,

    lap.quality_status,
    lap.quality_issues,
    s._ingested_at,
    s._silver_processed_at AS _hub_processed_at,
    'Europe/Paris' AS timezone,
    CURRENT_TIMESTAMP() AS _product_built_at
FROM {{ ref('hub_activities_svc__activity_splits') }} AS s
LEFT JOIN {{ ref('hub_activities_svc__fact_activity') }} AS a
    ON s.activity_id = a.activity_id
CROSS JOIN UNNEST(s.laps) AS lap
