{{
    config(
        materialized='view',
        tags=['garmin']
    )
}}

-- Master running activities view.
-- One row per running activity (running / trail_running / treadmill_running).
-- Consolidates all lake service tables into a single fully-denormalized record:
--   • Flat performance metrics (from svc_garmin__activities)
--   • Fastest splits / power zones (from svc_garmin__activities)
--   • HR zones          → ARRAY<STRUCT>  (from svc_garmin__activity_hr_zones)
--   • Laps              → ARRAY<STRUCT>  (from svc_garmin__activity_splits)
--   • Typed splits      → ARRAY<STRUCT>  (from svc_garmin__activity_splits)
--   • Split summaries   → ARRAY<STRUCT>  (from svc_garmin__activity_splits)
--   • Timeseries (~2s)  → ARRAY<STRUCT>  (from svc_garmin__activity_details.detailed_data)
--   • Weather           → STRUCT         (from svc_garmin__activity_weather)
--   • Sleep             → STRUCT         (from svc_garmin__sleep)
--   • Athletic context  → STRUCT         (training readiness / VO2max / weight)

WITH

-- ── 1. Base: running activities only ────────────────────────────────────────
activities AS (
    SELECT *
    FROM {{ ref('svc_garmin__activities') }}
    WHERE json_value(activitytype, '$.typeKey') IN (
        'running',
        'trail_running',
        'treadmill_running'
    )
),

-- ── 2. Activity details: timeseries JSON ────────────────────────────────────
activity_details AS (
    SELECT
        activityid,
        detailed_data,
        _ingested_at AS details_ingested_at
    FROM {{ ref('svc_garmin__activity_details') }}
),

-- ── 3. HR zones: aggregate into ARRAY per activity ──────────────────────────
hr_zones_agg AS (
    SELECT
        activityid,
        hr_zones
    FROM {{ ref('svc_garmin__activity_hr_zones') }}
),

-- ── 4. Splits: laps + typed_splits + split_summaries (already ARRAY<STRUCT>) ─
splits_agg AS (
    SELECT
        activityid,
        laps,
        typed_splits,
        split_summaries
    FROM {{ ref('svc_garmin__activity_splits') }}
),

-- ── 5. Weather ──────────────────────────────────────────────────────────────
weather AS (
    SELECT
        activityid,
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
    FROM {{ ref('svc_garmin__activity_weather') }}
),

-- ── 6. Sleep (night before the activity) ────────────────────────────────────
sleep AS (
    SELECT
        date,
        sleep_overall_score,
        sleep_overall_score_qualifier,
        sleep_quality_key,
        sleep_time_seconds,
        deep_sleep_seconds,
        light_sleep_seconds,
        rem_sleep_seconds,
        awake_sleep_seconds,
        avgovernighthrv,
        hrvstatus,
        restingheartrate,
        bodybatterychange,
        sleep_avg_spo2_value,
        sleep_lowest_spo2_value,
        sleep_avg_respiration,
        sleep_score_feedback,
        sleep_start_timestamp_gmt,
        sleep_end_timestamp_gmt
    FROM {{ ref('svc_garmin__sleep') }}
),

-- ── 7. Training readiness ────────────────────────────────────────────────────
training_readiness AS (
    SELECT
        calendardate,
        score,
        level,
        feedbackshort,
        sleepscorefactorpercent,
        hrvfactorpercent,
        stresshistoryfactorpercent,
        recoverytimefactorpercent,
        acwrfactorpercent,
        acuteload,
        recoverytime,
        hrvweeklyaverage
    FROM {{ ref('svc_garmin__training_readiness') }}
),

-- ── 8. VO2max: most recent measurement valid on the activity date ────────────
max_metrics AS (
    SELECT
        date,
        vo2_max,
        vo2_max_precise,
        vo2_max_calendar_date,
        lead(date) OVER (ORDER BY date) AS valid_until
    FROM {{ ref('svc_garmin__max_metrics') }}
    WHERE vo2_max IS NOT NULL
),

-- ── 9. Weight: most recent measurement valid on the activity date ────────────
weight AS (
    SELECT
        calendardate,
        weight,
        bodyfat,
        bmi,
        musclemass,
        bonemass,
        lead(calendardate) OVER (ORDER BY calendardate) AS valid_until
    FROM {{ ref('svc_garmin__weight') }}
    WHERE weight IS NOT NULL
),

-- ── 10. Training status (primary device, parsed from latestTrainingStatusData) ─
-- latestTrainingStatusData is a JSON object keyed by deviceId.
-- Extract the entry where primaryTrainingDevice = true.
training_status AS (
    SELECT
        date,
        (
            SELECT
                struct(
                    int64(ts_obj[k]['trainingStatus']) AS code,
                    string(ts_obj[k]['trainingStatusFeedbackPhrase']) AS feedback_phrase,
                    int64(ts_obj[k]['fitnessTrend']) AS fitness_trend,
                    string(ts_obj[k]['acuteTrainingLoadDTO']['acwrStatus']) AS acwr_status,
                    int64(ts_obj[k]['acuteTrainingLoadDTO']['acwrPercent']) AS acwr_percent,
                    int64(ts_obj[k]['acuteTrainingLoadDTO']['dailyTrainingLoadAcute']) AS daily_load_acute,
                    float64(ts_obj[k]['acuteTrainingLoadDTO']['dailyAcuteChronicWorkloadRatio']) AS daily_acwr_ratio
                )
            FROM unnest([safe.parse_json(latesttrainingstatusdata)]) AS ts_obj,
                unnest(json_keys(safe.parse_json(latesttrainingstatusdata))) AS k
            WHERE bool(ts_obj[k]['primaryTrainingDevice'])
            ORDER BY k
            LIMIT 1
        ) AS primary_status
    FROM {{ ref('svc_garmin__training_status') }}
)

SELECT

    -- ── Identity ─────────────────────────────────────────────────────────────
    a.activityid AS activity_id,
    a.activityuuid AS activity_uuid,
    a.activityname AS activity_name,
    a.description AS activity_description,
    a.locationname AS location_name,
    a.starttimelocal AS start_time_local,
    a.starttimegmt AS start_time_gmt,
    a.endtimegmt AS end_time_gmt,
    a.deviceid AS device_id,
    a.manufacturer,
    a.pr AS is_pr,
    a.favorite AS is_favorite,
    a.manualactivity AS is_manual,
    a.elevationcorrected AS is_elevation_corrected,
    a.eventtype AS event_type,
    a._ingested_at,
    date(a.starttimelocal) AS activity_date,
    json_value(a.activitytype, '$.typeKey') AS activity_type,

    -- ── Performance (flat STRUCT) ─────────────────────────────────────────────
    struct(
        -- Distance & time
        a.distance AS distance_m,
        a.duration AS duration_s,
        a.movingduration AS moving_duration_s,
        a.elapsedduration AS elapsed_duration_s,
        -- Pace & speed
        safe_divide(1000.0, a.averagespeed) / 60.0 AS avg_pace_min_per_km,
        a.averagespeed AS avg_speed_m_per_s,
        a.maxspeed AS max_speed_m_per_s,
        a.avggradeadjustedspeed AS avg_grade_adjusted_speed_m_per_s,
        -- Elevation
        a.elevationgain AS elevation_gain_m,
        a.elevationloss AS elevation_loss_m,
        a.avgelevation AS avg_elevation_m,
        a.minelevation AS min_elevation_m,
        a.maxelevation AS max_elevation_m,
        a.avgverticalspeed AS avg_vertical_speed_m_per_s,
        a.maxverticalspeed AS max_vertical_speed_m_per_s,
        -- Calories
        a.calories,
        a.bmrcalories AS bmr_calories,
        -- Heart rate
        a.averagehr AS avg_hr_bpm,
        a.maxhr AS max_hr_bpm,
        -- Cadence
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
        -- Respiration
        a.avgrespirationrate AS avg_respiration_rpm,
        a.minrespirationrate AS min_respiration_rpm,
        a.maxrespirationrate AS max_respiration_rpm,
        -- Temperature during effort
        a.mintemperature AS min_temp_celsius,
        a.maxtemperature AS max_temp_celsius,
        -- Training effect & load
        a.aerobictrainingeffect AS aerobic_te,
        a.aerobictrainingeffectmessage AS aerobic_te_message,
        a.anaerobictrainingeffect AS anaerobic_te,
        a.anaerobictrainingeffectmessage AS anaerobic_te_message,
        a.trainingeffectlabel AS training_effect_label,
        a.activitytrainingload AS training_load,
        a.vo2maxvalue AS vo2max_during_activity,
        -- Body battery
        a.differencebodybattery AS body_battery_delta,
        -- Counts
        a.steps,
        a.lapcount AS lap_count,
        a.vigorousintensityminutes AS vigorous_intensity_min,
        a.moderateintensityminutes AS moderate_intensity_min
    ) AS performance,

    -- ── Fastest splits (STRUCT) ───────────────────────────────────────────────
    struct(
        a.fastestsplit_1000 AS dist_1000m_s,
        a.fastestsplit_1609 AS dist_1609m_s,
        a.fastestsplit_5000 AS dist_5000m_s,
        a.fastestsplit_10000 AS dist_10000m_s,
        a.fastestsplit_21098 AS dist_21098m_s,
        a.fastestsplit_42195 AS dist_42195m_s
    ) AS fastest_splits,

    -- ── GPS coordinates (STRUCT) ──────────────────────────────────────────────
    struct(
        a.startlatitude AS start_latitude,
        a.startlongitude AS start_longitude,
        a.endlatitude AS end_latitude,
        a.endlongitude AS end_longitude
    ) AS gps,

    -- ── HR zones (ARRAY<STRUCT>, 1 entry per zone 1-5) ───────────────────────
    coalesce(hrz.hr_zones, []) AS hr_zones,

    -- ── Power zones (ARRAY<STRUCT>, built from flat columns) ─────────────────
    [
        struct(1 AS zone_number, a.powertimeinzone_1 AS secs_in_zone),
        struct(2 AS zone_number, a.powertimeinzone_2 AS secs_in_zone),
        struct(3 AS zone_number, a.powertimeinzone_3 AS secs_in_zone),
        struct(4 AS zone_number, a.powertimeinzone_4 AS secs_in_zone),
        struct(5 AS zone_number, a.powertimeinzone_5 AS secs_in_zone)
    ] AS power_zones,

    -- ── Laps (ARRAY<STRUCT>, 1 entry per km/lap) ──────────────────────────────
    coalesce(sp.laps, []) AS laps,

    -- ── Typed splits (ARRAY<STRUCT>, by movement type: RWD_RUN/WALK/STAND) ───
    coalesce(sp.typed_splits, []) AS typed_splits,

    -- ── Split summaries (ARRAY<STRUCT>, by split category) ───────────────────
    coalesce(sp.split_summaries, []) AS split_summaries,

    -- ── Timeseries ~2s (ARRAY<STRUCT>, one entry per Garmin metric point) ────
    coalesce(
        array(
            SELECT AS STRUCT
                cast(json_value(m, '$.sumElapsedDuration') AS float64) AS elapsed_s,
                cast(json_value(m, '$.sumDuration') AS float64) AS duration_s,
                cast(json_value(m, '$.sumMovingDuration') AS float64) AS moving_duration_s,
                cast(json_value(m, '$.sumDistance') AS float64) AS cum_distance_m,
                cast(json_value(m, '$.directSpeed') AS float64) AS speed_m_per_s,
                cast(json_value(m, '$.directHeartRate') AS float64) AS heart_rate_bpm,
                cast(json_value(m, '$.directDoubleCadence') AS float64) AS cadence_spm,
                cast(json_value(m, '$.directFractionalCadence') AS float64) AS fractional_cadence,
                cast(json_value(m, '$.directBodyBattery') AS float64) AS body_battery,
                cast(json_value(m, '$.directLatitude') AS float64) AS latitude,
                cast(json_value(m, '$.directLongitude') AS float64) AS longitude,
                cast(json_value(m, '$.directAltitude') AS float64) AS altitude_m,
                cast(json_value(m, '$.directVerticalSpeed') AS float64) AS vertical_speed_m_per_s,
                cast(json_value(m, '$.directGroundContactTime') AS float64) AS ground_contact_ms,
                cast(json_value(m, '$.directVerticalOscillation') AS float64) AS vertical_oscillation_mm,
                cast(json_value(m, '$.directVerticalRatio') AS float64) AS vertical_ratio,
                cast(json_value(m, '$.directStrideLength') AS float64) AS stride_length_m,
                cast(json_value(m, '$.directPower') AS float64) AS power_w,
                cast(json_value(m, '$.directAirTemperature') AS float64) AS air_temp_celsius,
                cast(json_value(m, '$.directGrade') AS float64) AS grade_pct,
                timestamp_millis(
                    cast(floor(cast(json_value(m, '$.directTimestamp') AS float64)) AS int64)
                ) AS timestamp_gmt
            FROM
                unnest(
                    json_query_array(ad.detailed_data, '$.activityDetailMetrics')
                ) AS m
            WHERE json_value(m, '$.directTimestamp') IS NOT NULL
            ORDER BY cast(json_value(m, '$.directTimestamp') AS float64)
        ),
        []
    ) AS timeseries,

    -- ── Weather conditions at start of activity (STRUCT) ─────────────────────
    struct(
        w.weather_temp AS temp_celsius,
        w.weather_apparent_temp AS apparent_temp_celsius,
        w.weather_dew_point AS dew_point_celsius,
        w.weather_relative_humidity AS humidity_pct,
        w.weather_wind_speed AS wind_speed_kph,
        w.weather_wind_direction AS wind_direction_deg,
        w.weather_wind_direction_compass AS wind_direction_compass,
        w.weather_type,
        w.weather_latitude AS latitude,
        w.weather_longitude AS longitude,
        w.weather_station_name AS station_name
    ) AS weather,

    -- ── Sleep (night before the activity) (STRUCT) ────────────────────────────
    struct(
        s.date AS sleep_date,
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

    -- ── Athletic context on the day of the activity (STRUCT) ─────────────────
    struct(
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
        tr.recoverytime AS recovery_time_hours,
        tr.hrvweeklyaverage AS hrv_weekly_average,
        -- Training status — primary device, parsed from latestTrainingStatusData
        ts.primary_status.code AS training_status_code,
        ts.primary_status.feedback_phrase AS training_status_feedback,
        ts.primary_status.fitness_trend AS fitness_trend,
        ts.primary_status.acwr_status AS acwr_status,
        ts.primary_status.acwr_percent AS acwr_percent,
        ts.primary_status.daily_load_acute AS ts_acute_load,
        ts.primary_status.daily_acwr_ratio AS ts_acwr_ratio,
        -- VO2max (most recent valid measurement)
        mm.vo2_max,
        mm.vo2_max_precise,
        mm.vo2_max_calendar_date AS vo2max_date,
        -- Body composition (most recent valid measurement)
        safe_divide(wt.weight, 1000.0) AS weight_kg,
        wt.bodyfat AS body_fat_pct,
        wt.bmi,
        wt.musclemass AS muscle_mass_g,
        wt.bonemass AS bone_mass_g
    ) AS athletic_context

FROM activities AS a
LEFT JOIN activity_details AS ad
    ON a.activityid = ad.activityid
LEFT JOIN hr_zones_agg AS hrz
    ON a.activityid = hrz.activityid
LEFT JOIN splits_agg AS sp
    ON a.activityid = sp.activityid
LEFT JOIN weather AS w
    ON a.activityid = w.activityid
LEFT JOIN sleep AS s
    ON s.date = date(a.starttimelocal)
LEFT JOIN training_readiness AS tr
    ON tr.calendardate = date(a.starttimelocal)
LEFT JOIN max_metrics AS mm
    ON
        mm.date <= date(a.starttimelocal)
        AND (mm.valid_until IS NULL OR mm.valid_until > date(a.starttimelocal))
LEFT JOIN weight AS wt
    ON
        wt.calendardate <= date(a.starttimelocal)
        AND (wt.valid_until IS NULL OR wt.valid_until > date(a.starttimelocal))
LEFT JOIN training_status AS ts
    ON ts.date = date(a.starttimelocal)
