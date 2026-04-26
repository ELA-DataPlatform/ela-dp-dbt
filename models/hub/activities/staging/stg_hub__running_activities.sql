{{
    config(
        materialized='view',
        tags=['garmin']
    )
}}

with
activities as (
    select *
    from {{ ref('svc_garmin__activities') }}
    where json_value(activityType, '$.typeKey') in ('running', 'trail_running', 'treadmill_running')
),

hr_zones as (
    select activityId, hr_zones
    from {{ ref('svc_garmin__activity_hr_zones') }}
),

weather as (
    select
        activityId,
        weather_issue_date,
        weather_temp,
        weather_apparent_temp,
        weather_dew_point,
        weather_relative_humidity,
        weather_wind_speed,
        weather_wind_direction,
        weather_wind_direction_compass,
        weather_type,
        weather_latitude,
        weather_longitude,
        weather_station_name
    from {{ ref('svc_garmin__activity_weather') }}
),

sleep as (
    select
        date,
        sleep_overall_score,
        sleep_overall_score_qualifier,
        sleep_quality_key,
        sleep_time_seconds,
        deep_sleep_seconds,
        light_sleep_seconds,
        rem_sleep_seconds,
        awake_sleep_seconds,
        avgOvernightHrv,
        hrvStatus,
        restingHeartRate,
        bodyBatteryChange,
        sleep_avg_spo2_value,
        sleep_lowest_spo2_value,
        sleep_avg_respiration,
        sleep_score_feedback,
        sleep_start_timestamp_gmt,
        sleep_end_timestamp_gmt
    from {{ ref('svc_garmin__sleep') }}
),

training_readiness as (
    select
        calendarDate,
        score,
        level,
        feedbackShort,
        sleepScoreFactorPercent,
        hrvFactorPercent,
        stressHistoryFactorPercent,
        recoveryTimeFactorPercent,
        acwrFactorPercent,
        acuteLoad,
        recoveryTime,
        hrvWeeklyAverage
    from {{ ref('svc_garmin__training_readiness') }}
),

-- Keep only the most recent VO2max measurement per date range
max_metrics as (
    select
        date,
        vo2_max,
        vo2_max_precise,
        vo2_max_calendar_date,
        lead(date) over (order by date) as valid_until
    from {{ ref('svc_garmin__max_metrics') }}
    where vo2_max is not null
),

-- Keep only the most recent weight measurement per date range
weight as (
    select
        calendarDate,
        weight,
        bodyFat,
        bmi,
        muscleMass,
        boneMass,
        lead(calendarDate) over (order by calendarDate) as valid_until
    from {{ ref('svc_garmin__weight') }}
    where weight is not null
),

training_status as (
    select date, latestTrainingStatusData
    from {{ ref('svc_garmin__training_status') }}
)

select
    -- Identity
    a.activityId                                      as activity_id,
    a.activityName                                    as activity_name,
    a.locationName                                    as location_name,
    a.startTimeLocal                                  as start_time_local,
    a.startTimeGMT                                    as start_time_gmt,
    date(a.startTimeLocal)                            as activity_date,
    json_value(a.activityType, '$.typeKey')           as activity_type,
    a._ingested_at,

    -- HR zones (ARRAY of STRUCTs — one entry per zone)
    coalesce(hrz.hr_zones, [])                        as hr_zones,

    -- Split summaries (ARRAY of STRUCTs — one entry per split type)
    a.split_summaries,

    -- Performance metrics
    struct(
        a.distance                                    as distance_m,
        a.duration                                    as duration_s,
        a.movingDuration                              as moving_duration_s,
        a.elapsedDuration                             as elapsed_duration_s,
        safe_divide(1000.0, a.averageSpeed) / 60.0    as avg_pace_min_per_km,
        a.averageSpeed                                as avg_speed_m_per_s,
        a.maxSpeed                                    as max_speed_m_per_s,
        a.elevationGain                               as elevation_gain_m,
        a.elevationLoss                               as elevation_loss_m,
        a.calories,
        a.averageHR                                   as avg_hr_bpm,
        a.maxHR                                       as max_hr_bpm,
        a.averageRunningCadenceInStepsPerMinute        as avg_cadence_spm,
        a.maxRunningCadenceInStepsPerMinute            as max_cadence_spm,
        a.avgPower                                    as avg_power_w,
        a.maxPower                                    as max_power_w,
        a.normPower                                   as norm_power_w,
        a.avgStrideLength                             as avg_stride_length_m,
        a.avgGroundContactTime                        as avg_ground_contact_ms,
        a.avgVerticalOscillation                      as avg_vertical_oscillation_mm,
        a.avgVerticalRatio                            as avg_vertical_ratio,
        a.avgGradeAdjustedSpeed                       as avg_grade_adjusted_speed_m_per_s,
        a.aerobicTrainingEffect                       as aerobic_training_effect,
        a.aerobicTrainingEffectMessage                as aerobic_te_message,
        a.anaerobicTrainingEffect                     as anaerobic_training_effect,
        a.anaerobicTrainingEffectMessage              as anaerobic_te_message,
        a.activityTrainingLoad                        as training_load,
        a.vO2MaxValue                                 as vo2max_during_activity,
        a.steps,
        a.lapCount                                    as lap_count,
        a.pr                                          as is_pr
    )                                                 as performance,

    -- GPS coordinates
    struct(
        a.startLatitude                               as start_latitude,
        a.startLongitude                              as start_longitude,
        a.endLatitude                                 as end_latitude,
        a.endLongitude                                as end_longitude,
        a.avgElevation                                as avg_elevation_m,
        a.minElevation                                as min_elevation_m,
        a.maxElevation                                as max_elevation_m
    )                                                 as gps,

    -- Weather conditions during activity
    struct(
        w.weather_temp                                as temp_celsius,
        w.weather_apparent_temp                       as apparent_temp_celsius,
        w.weather_dew_point                           as dew_point_celsius,
        w.weather_relative_humidity                   as humidity_pct,
        w.weather_wind_speed                          as wind_speed_kph,
        w.weather_wind_direction                      as wind_direction_deg,
        w.weather_wind_direction_compass              as wind_direction_compass,
        w.weather_type,
        w.weather_latitude                            as latitude,
        w.weather_longitude                           as longitude,
        w.weather_station_name                        as station_name
    )                                                 as weather,

    -- Sleep from the night before the activity
    -- sleep.date = the morning wake-up date, so it equals the activity date
    struct(
        s.date                                        as sleep_date,
        s.sleep_overall_score                         as overall_score,
        s.sleep_overall_score_qualifier               as score_qualifier,
        s.sleep_quality_key                           as quality_key,
        s.sleep_time_seconds                          as total_sleep_s,
        s.deep_sleep_seconds                          as deep_s,
        s.light_sleep_seconds                         as light_s,
        s.rem_sleep_seconds                           as rem_s,
        s.awake_sleep_seconds                         as awake_s,
        s.avgOvernightHrv                             as avg_overnight_hrv,
        s.hrvStatus                                   as hrv_status,
        s.restingHeartRate                            as resting_hr_bpm,
        s.bodyBatteryChange                           as body_battery_change,
        s.sleep_avg_spo2_value                        as avg_spo2,
        s.sleep_lowest_spo2_value                     as lowest_spo2,
        s.sleep_avg_respiration                       as avg_respiration_rpm,
        s.sleep_score_feedback                        as score_feedback,
        s.sleep_start_timestamp_gmt                   as start_gmt,
        s.sleep_end_timestamp_gmt                     as end_gmt
    )                                                 as sleep,

    -- Athletic context on the day of the activity
    struct(
        -- Training readiness score and contributing factors
        tr.score                                      as readiness_score,
        tr.level                                      as readiness_level,
        tr.feedbackShort                              as readiness_feedback,
        tr.sleepScoreFactorPercent                    as readiness_sleep_factor_pct,
        tr.hrvFactorPercent                           as readiness_hrv_factor_pct,
        tr.stressHistoryFactorPercent                 as readiness_stress_factor_pct,
        tr.recoveryTimeFactorPercent                  as readiness_recovery_factor_pct,
        tr.acwrFactorPercent                          as readiness_acwr_factor_pct,
        tr.acuteLoad                                  as acute_training_load,
        tr.recoveryTime                               as recovery_time_hours,
        tr.hrvWeeklyAverage                           as hrv_weekly_average,
        -- Most recent VO2max before the activity
        mm.vo2_max,
        mm.vo2_max_precise,
        mm.vo2_max_calendar_date                      as vo2max_date,
        -- Most recent weight before the activity (weight stored in grams → convert to kg)
        safe_divide(wt.weight, 1000.0)                as weight_kg,
        wt.bodyFat                                    as body_fat_pct,
        wt.bmi,
        wt.muscleMass                                 as muscle_mass_g,
        wt.boneMass                                   as bone_mass_g,
        -- Training status raw JSON (keyed by deviceId — use for downstream parsing)
        ts.latestTrainingStatusData                   as training_status_json
    )                                                 as athletic_context

from activities a
left join hr_zones hrz
    on hrz.activityId = a.activityId
left join weather w
    on w.activityId = a.activityId
-- Sleep: date = morning wake-up date = activity date
left join sleep s
    on s.date = date(a.startTimeLocal)
left join training_readiness tr
    on tr.calendarDate = date(a.startTimeLocal)
-- Most recent VO2max measurement valid on the activity date
left join max_metrics mm
    on mm.date <= date(a.startTimeLocal)
    and (mm.valid_until is null or mm.valid_until > date(a.startTimeLocal))
-- Most recent weight measurement valid on the activity date
left join weight wt
    on wt.calendarDate <= date(a.startTimeLocal)
    and (wt.valid_until is null or wt.valid_until > date(a.startTimeLocal))
left join training_status ts
    on ts.date = date(a.startTimeLocal)
