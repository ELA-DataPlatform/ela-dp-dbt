{{
    config(
        alias='activities',
        materialized='table',
        tags=['garmin', 'data4agent']
    )
}}

SELECT  -- noqa: ST06
    a.activityid AS activity_id,
    a.activityname AS activity_name,
    a.activity_type.type_key AS activity_type,
    DATE(a.starttimelocal) AS activity_date,
    a.starttimelocal AS start_time_local,
    a.starttimegmt AS start_time_gmt,
    a.endtimegmt AS end_time_gmt,
    a.locationname AS location_name,

    -- Distance & time
    a.distance AS distance_m,
    SAFE_DIVIDE(a.distance, 1000.0) AS distance_km,
    a.duration AS duration_s,
    a.movingduration AS moving_duration_s,
    a.elapsedduration AS elapsed_duration_s,

    -- Pace & speed
    {{ speed_mps_to_pace_min_per_km('a.averagespeed') }} AS avg_pace_min_per_km,
    a.averagespeed AS avg_speed_m_per_s,
    a.maxspeed AS max_speed_m_per_s,

    -- Elevation
    a.elevationgain AS elevation_gain_m,
    a.elevationloss AS elevation_loss_m,
    a.minelevation AS min_elevation_m,
    a.maxelevation AS max_elevation_m,

    -- Heart rate
    a.averagehr AS avg_hr_bpm,
    a.maxhr AS max_hr_bpm,

    -- Cadence (running)
    a.averagerunningcadenceinstepsperminute AS avg_cadence_spm,
    a.maxrunningcadenceinstepsperminute AS max_cadence_spm,

    -- Power
    a.avgpower AS avg_power_w,
    a.maxpower AS max_power_w,
    a.normpower AS norm_power_w,

    -- Running dynamics
    a.avgstridelength AS avg_stride_length_m,
    a.avggroundcontacttime AS avg_ground_contact_ms,
    a.avgverticaloscillation AS avg_vertical_oscillation_mm,
    a.avgverticalratio AS avg_vertical_ratio,

    -- Calories & training effect
    a.calories,
    a.bmrcalories AS bmr_calories,
    a.aerobictrainingeffect AS aerobic_te,
    a.anaerobictrainingeffect AS anaerobic_te,
    a.trainingeffectlabel AS training_effect_label,
    a.activitytrainingload AS training_load,
    a.vo2maxvalue AS vo2max_during_activity,

    -- Counts
    a.steps,
    a.lapcount AS lap_count,

    -- Flags
    a.pr AS is_pr,
    a.favorite AS is_favorite,
    a.manualactivity AS is_manual,

    a._ingested_at
FROM {{ ref('dlk_garmin_svc__activities') }} AS a
WHERE
    a.starttimelocal IS NOT NULL
    AND DATE(a.starttimelocal) >= '2025-01-01'
ORDER BY a.starttimelocal DESC
