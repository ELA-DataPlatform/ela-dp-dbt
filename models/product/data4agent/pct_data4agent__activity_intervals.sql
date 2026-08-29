{{
    config(
        alias='activity_intervals',
        materialized='table',
        tags=['garmin', 'data4agent']
    )
}}

SELECT  -- noqa: ST06
    s.activityid AS activity_id,
    a.activityname AS activity_name,
    a.activity_type.type_key AS activity_type,
    DATE(a.starttimelocal) AS activity_date,
    lap.lap_index,
    lap.message_index,
    lap.intensity_type,

    -- Time
    lap.start_time_gmt,
    lap.duration AS duration_s,
    lap.moving_duration AS moving_duration_s,
    lap.elapsed_duration AS elapsed_duration_s,

    -- Distance & speed
    lap.distance AS distance_m,
    SAFE_DIVIDE(lap.distance, 1000.0) AS distance_km,
    lap.average_speed AS avg_speed_m_per_s,
    lap.average_moving_speed AS avg_moving_speed_m_per_s,
    lap.max_speed AS max_speed_m_per_s,
    {{ speed_mps_to_pace_min_per_km('lap.average_speed') }} AS avg_pace_min_per_km,
    lap.avg_grade_adjusted_speed AS avg_grade_adjusted_speed_m_per_s,

    -- Elevation
    lap.elevation_gain AS elevation_gain_m,
    lap.elevation_loss AS elevation_loss_m,
    lap.min_elevation AS min_elevation_m,
    lap.max_elevation AS max_elevation_m,
    lap.max_vertical_speed AS max_vertical_speed_m_per_s,

    -- Heart rate
    lap.average_hr AS avg_hr_bpm,
    lap.max_hr AS max_hr_bpm,

    -- Cadence
    lap.average_run_cadence AS avg_cadence_spm,
    lap.max_run_cadence AS max_cadence_spm,

    -- Power
    lap.average_power AS avg_power_w,
    lap.max_power AS max_power_w,
    lap.min_power AS min_power_w,
    lap.normalized_power AS norm_power_w,
    lap.total_work AS total_work_kj,

    -- Running dynamics
    lap.stride_length AS avg_stride_length_m,
    lap.ground_contact_time AS avg_ground_contact_ms,
    lap.vertical_oscillation AS avg_vertical_oscillation_mm,
    lap.vertical_ratio AS avg_vertical_ratio,

    -- Calories & temperature
    lap.calories,
    lap.bmr_calories,
    lap.average_temperature AS avg_temp_celsius,
    lap.min_temperature AS min_temp_celsius,
    lap.max_temperature AS max_temp_celsius,

    -- GPS
    lap.start_latitude,
    lap.start_longitude,
    lap.end_latitude,
    lap.end_longitude,

    s._ingested_at
FROM {{ ref('dlk_garmin_svc__activity_splits') }} AS s
LEFT JOIN {{ ref('dlk_garmin_svc__activities') }} AS a
    ON s.activityid = a.activityid
CROSS JOIN UNNEST(s.laps) AS lap
WHERE DATE(a.starttimelocal) >= '2025-01-01'
