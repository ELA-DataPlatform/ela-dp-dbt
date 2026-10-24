{{ config(materialized='view') }}

-- One canonical activity, all sports, for the platform's current Garmin account.
-- Local times were stored as TIMESTAMP in bronze; DATETIME removes that false UTC label.
WITH classified AS (
    SELECT
        a.*,
        CASE
            WHEN a.activity_type.type_key IN (
                'running', 'trail_running', 'treadmill_running', 'track_running',
                'ultra_run', 'virtual_run', 'obstacle_run', 'indoor_track'
            ) THEN 'running'
            WHEN a.activity_type.type_key IN (
                'cycling', 'indoor_cycling', 'road_biking', 'mountain_biking',
                'gravel_cycling', 'virtual_ride', 'e_bike_fitness', 'e_bike_mountain'
            ) THEN 'cycling'
            WHEN a.activity_type.type_key IN ('strength_training', 'fitness_equipment') THEN 'strength'
            WHEN a.activity_type.type_key IN ('walking', 'hiking', 'speed_walking') THEN 'walking'
            WHEN a.activity_type.type_key IN ('lap_swimming', 'open_water_swimming', 'swimming') THEN 'swimming'
            ELSE 'other'
        END AS sport_family
    FROM {{ ref('dlk_garmin_svc__activities') }} AS a
),

qualified AS (
    SELECT
        a.*,
        ARRAY_CONCAT(
            IF(
                a.distance IS NOT NULL AND {{ garmin_activity_number('a.distance', 0, none) }} IS NULL,
                ['invalid_distance_m'],
                ARRAY<STRING>[]
            ),
            IF(
                a.duration IS NOT NULL AND {{ garmin_activity_number('a.duration', 0, none) }} IS NULL,
                ['invalid_duration_s'],
                ARRAY<STRING>[]
            ),
            IF(
                a.movingduration IS NOT NULL AND {{ garmin_activity_number('a.movingduration', 0, none) }} IS NULL,
                ['invalid_moving_duration_s'],
                ARRAY<STRING>[]
            ),
            IF(
                a.elapsedduration IS NOT NULL AND {{ garmin_activity_number('a.elapsedduration', 0, none) }} IS NULL,
                ['invalid_elapsed_duration_s'],
                ARRAY<STRING>[]
            ),
            IF(
                a.averagespeed IS NOT NULL AND {{ garmin_activity_number('a.averagespeed', 0, none) }} IS NULL,
                ['invalid_avg_speed_m_per_s'],
                ARRAY<STRING>[]
            ),
            IF(
                a.maxspeed IS NOT NULL AND {{ garmin_activity_number('a.maxspeed', 0, none) }} IS NULL,
                ['invalid_max_speed_m_per_s'],
                ARRAY<STRING>[]
            ),
            IF(
                a.avggradeadjustedspeed IS NOT NULL
                AND {{ garmin_activity_number('a.avggradeadjustedspeed', 0, none) }} IS NULL,
                ['invalid_avg_grade_adjusted_speed_m_per_s'],
                ARRAY<STRING>[]
            ),
            IF(
                a.elevationgain IS NOT NULL AND {{ garmin_activity_number('a.elevationgain', 0, none) }} IS NULL,
                ['invalid_elevation_gain_m'],
                ARRAY<STRING>[]
            ),
            IF(
                a.elevationloss IS NOT NULL AND {{ garmin_activity_number('a.elevationloss', 0, none) }} IS NULL,
                ['invalid_elevation_loss_m'],
                ARRAY<STRING>[]
            ),
            IF(
                a.calories IS NOT NULL AND {{ garmin_activity_number('a.calories', 0, none) }} IS NULL,
                ['invalid_calories'],
                ARRAY<STRING>[]
            ),
            IF(
                a.bmrcalories IS NOT NULL AND {{ garmin_activity_number('a.bmrcalories', 0, none) }} IS NULL,
                ['invalid_bmr_calories'],
                ARRAY<STRING>[]
            ),
            IF(
                a.averagehr IS NOT NULL AND {{ garmin_activity_number('a.averagehr', 0, none) }} IS NULL,
                ['invalid_avg_hr_bpm'],
                ARRAY<STRING>[]
            ),
            IF(
                a.maxhr IS NOT NULL AND {{ garmin_activity_number('a.maxhr', 0, none) }} IS NULL,
                ['invalid_max_hr_bpm'],
                ARRAY<STRING>[]
            ),
            IF(
                a.avgpower IS NOT NULL AND {{ garmin_activity_number('a.avgpower', 0, none) }} IS NULL,
                ['invalid_avg_power_w'],
                ARRAY<STRING>[]
            ),
            IF(
                a.maxpower IS NOT NULL AND {{ garmin_activity_number('a.maxpower', 0, none) }} IS NULL,
                ['invalid_max_power_w'],
                ARRAY<STRING>[]
            ),
            IF(
                a.normpower IS NOT NULL AND {{ garmin_activity_number('a.normpower', 0, none) }} IS NULL,
                ['invalid_norm_power_w'],
                ARRAY<STRING>[]
            ),
            IF(
                a.avgrespirationrate IS NOT NULL
                AND {{ garmin_activity_number('a.avgrespirationrate', 0, none) }} IS NULL,
                ['invalid_avg_respiration_rpm'],
                ARRAY<STRING>[]
            ),
            IF(
                a.minrespirationrate IS NOT NULL
                AND {{ garmin_activity_number('a.minrespirationrate', 0, none) }} IS NULL,
                ['invalid_min_respiration_rpm'],
                ARRAY<STRING>[]
            ),
            IF(
                a.maxrespirationrate IS NOT NULL
                AND {{ garmin_activity_number('a.maxrespirationrate', 0, none) }} IS NULL,
                ['invalid_max_respiration_rpm'],
                ARRAY<STRING>[]
            ),
            IF(
                a.activitytrainingload IS NOT NULL
                AND {{ garmin_activity_number('a.activitytrainingload', 0, none) }} IS NULL,
                ['invalid_training_load'],
                ARRAY<STRING>[]
            ),
            IF(
                a.vo2maxvalue IS NOT NULL AND {{ garmin_activity_number('a.vo2maxvalue', 0, none) }} IS NULL,
                ['invalid_vo2max_during_activity'],
                ARRAY<STRING>[]
            ),
            IF(
                a.steps IS NOT NULL AND {{ garmin_activity_number('a.steps', 0, none) }} IS NULL,
                ['invalid_steps'],
                ARRAY<STRING>[]
            ),
            IF(
                a.lapcount IS NOT NULL AND {{ garmin_activity_number('a.lapcount', 0, none) }} IS NULL,
                ['invalid_lap_count'],
                ARRAY<STRING>[]
            ),
            IF(
                a.vigorousintensityminutes IS NOT NULL
                AND {{ garmin_activity_number('a.vigorousintensityminutes', 0, none) }} IS NULL,
                ['invalid_vigorous_intensity_min'],
                ARRAY<STRING>[]
            ),
            IF(
                a.moderateintensityminutes IS NOT NULL
                AND {{ garmin_activity_number('a.moderateintensityminutes', 0, none) }} IS NULL,
                ['invalid_moderate_intensity_min'],
                ARRAY<STRING>[]
            ),
            IF(
                a.avgelevation IS NOT NULL AND {{ garmin_activity_number('a.avgelevation', none, none) }} IS NULL,
                ['invalid_avg_elevation_m'],
                ARRAY<STRING>[]
            ),
            IF(
                a.minelevation IS NOT NULL AND {{ garmin_activity_number('a.minelevation', none, none) }} IS NULL,
                ['invalid_min_elevation_m'],
                ARRAY<STRING>[]
            ),
            IF(
                a.maxelevation IS NOT NULL AND {{ garmin_activity_number('a.maxelevation', none, none) }} IS NULL,
                ['invalid_max_elevation_m'],
                ARRAY<STRING>[]
            ),
            IF(
                a.avgverticalspeed IS NOT NULL
                AND {{ garmin_activity_number('a.avgverticalspeed', none, none) }} IS NULL,
                ['invalid_avg_vertical_speed_m_per_s'],
                ARRAY<STRING>[]
            ),
            IF(
                a.maxverticalspeed IS NOT NULL
                AND {{ garmin_activity_number('a.maxverticalspeed', none, none) }} IS NULL,
                ['invalid_max_vertical_speed_m_per_s'],
                ARRAY<STRING>[]
            ),
            IF(
                a.mintemperature IS NOT NULL AND {{ garmin_activity_number('a.mintemperature', none, none) }} IS NULL,
                ['invalid_min_temp_celsius'],
                ARRAY<STRING>[]
            ),
            IF(
                a.maxtemperature IS NOT NULL AND {{ garmin_activity_number('a.maxtemperature', none, none) }} IS NULL,
                ['invalid_max_temp_celsius'],
                ARRAY<STRING>[]
            ),
            IF(
                a.differencebodybattery IS NOT NULL
                AND {{ garmin_activity_number('a.differencebodybattery', none, none) }} IS NULL,
                ['invalid_body_battery_delta'],
                ARRAY<STRING>[]
            ),
            IF(
                a.aerobictrainingeffect IS NOT NULL
                AND {{ garmin_activity_number('a.aerobictrainingeffect', 0, 5) }} IS NULL,
                ['invalid_aerobic_te'],
                ARRAY<STRING>[]
            ),
            IF(
                a.anaerobictrainingeffect IS NOT NULL
                AND {{ garmin_activity_number('a.anaerobictrainingeffect', 0, 5) }} IS NULL,
                ['invalid_anaerobic_te'],
                ARRAY<STRING>[]
            ),
            IF(
                a.averagerunningcadenceinstepsperminute IS NOT NULL
                AND {{ garmin_activity_number('a.averagerunningcadenceinstepsperminute', 0, none) }} IS NULL,
                ['invalid_avg_cadence_spm'],
                ARRAY<STRING>[]
            ),
            IF(
                a.maxrunningcadenceinstepsperminute IS NOT NULL
                AND {{ garmin_activity_number('a.maxrunningcadenceinstepsperminute', 0, none) }} IS NULL,
                ['invalid_max_cadence_spm'],
                ARRAY<STRING>[]
            ),
            IF(
                a.avggroundcontacttime IS NOT NULL
                AND {{ garmin_activity_number('a.avggroundcontacttime', 0, none) }} IS NULL,
                ['invalid_avg_ground_contact_ms'],
                ARRAY<STRING>[]
            ),
            IF(
                a.avgverticalratio IS NOT NULL AND {{ garmin_activity_number('a.avgverticalratio', 0, 100) }} IS NULL,
                ['invalid_avg_vertical_ratio'],
                ARRAY<STRING>[]
            ),
            IF(
                a.avgstridelength IS NOT NULL AND {{ garmin_activity_number('a.avgstridelength', 0, none) }} IS NULL,
                ['invalid_avg_stride_length_m'],
                ARRAY<STRING>[]
            ),
            IF(
                a.avgverticaloscillation IS NOT NULL
                AND {{ garmin_activity_number('a.avgverticaloscillation', 0, none) }} IS NULL,
                ['invalid_avg_vertical_oscillation_mm'],
                ARRAY<STRING>[]
            ),
            IF(
                a.fastestsplit_1000 IS NOT NULL
                AND {{ garmin_activity_number('a.fastestsplit_1000', 0, none) }} IS NULL,
                ['invalid_dist_1000m_s'],
                ARRAY<STRING>[]
            ),
            IF(
                a.fastestsplit_1609 IS NOT NULL
                AND {{ garmin_activity_number('a.fastestsplit_1609', 0, none) }} IS NULL,
                ['invalid_dist_1609m_s'],
                ARRAY<STRING>[]
            ),
            IF(
                a.fastestsplit_5000 IS NOT NULL
                AND {{ garmin_activity_number('a.fastestsplit_5000', 0, none) }} IS NULL,
                ['invalid_dist_5000m_s'],
                ARRAY<STRING>[]
            ),
            IF(
                a.fastestsplit_10000 IS NOT NULL
                AND {{ garmin_activity_number('a.fastestsplit_10000', 0, none) }} IS NULL,
                ['invalid_dist_10000m_s'],
                ARRAY<STRING>[]
            ),
            IF(
                a.fastestsplit_21098 IS NOT NULL
                AND {{ garmin_activity_number('a.fastestsplit_21098', 0, none) }} IS NULL,
                ['invalid_dist_21098m_s'],
                ARRAY<STRING>[]
            ),
            IF(
                a.fastestsplit_42195 IS NOT NULL
                AND {{ garmin_activity_number('a.fastestsplit_42195', 0, none) }} IS NULL,
                ['invalid_dist_42195m_s'],
                ARRAY<STRING>[]
            ),
            IF(
                a.startlatitude IS NOT NULL AND {{ garmin_activity_number('a.startlatitude', -90, 90) }} IS NULL,
                ['invalid_start_latitude'],
                ARRAY<STRING>[]
            ),
            IF(
                a.startlongitude IS NOT NULL AND {{ garmin_activity_number('a.startlongitude', -180, 180) }} IS NULL,
                ['invalid_start_longitude'],
                ARRAY<STRING>[]
            ),
            IF(
                a.endlatitude IS NOT NULL AND {{ garmin_activity_number('a.endlatitude', -90, 90) }} IS NULL,
                ['invalid_end_latitude'],
                ARRAY<STRING>[]
            ),
            IF(
                a.endlongitude IS NOT NULL AND {{ garmin_activity_number('a.endlongitude', -180, 180) }} IS NULL,
                ['invalid_end_longitude'],
                ARRAY<STRING>[]
            ),

            IF(a.totalreps < 0, ['invalid_total_reps'], ARRAY<STRING>[]),
            IF(a.totalsets < 0, ['invalid_total_sets'], ARRAY<STRING>[]),
            IF(a.activesets < 0, ['invalid_active_sets'], ARRAY<STRING>[])
        ) AS _metric_issues
    FROM classified AS a
)

SELECT
    a.activityid AS activity_id,
    CONCAT('garmin:', COALESCE(CAST(a.ownerid AS STRING), 'account'), ':', CAST(a.activityid AS STRING))
        AS activity_key,
    DATE(a.starttimelocal) AS activity_date,
    STRUCT(
        a.activityuuid AS activity_uuid,
        a.activityname AS activity_name,
        a.description AS activity_description,
        a.locationname AS location_name,
        a.deviceid AS device_id,
        a.manufacturer AS manufacturer,
        CAST(a.ownerid AS STRING) AS source_user_id,
        'user' AS identity_scope,
        'garmin' AS source_system
    ) AS identity,
    STRUCT(
        a.sport_family AS family,
        a.activity_type.type_key AS type_key,
        a.activity_type.type_id AS type_id,
        a.activity_type.parent_type_id AS parent_type_id,
        a.sporttypeid AS source_sport_type_id,
        a.sport_family != 'other' AS is_classified
    ) AS sport,
    STRUCT(
        a.starttimegmt AS start_at_utc,
        IF(a.endtimegmt >= a.starttimegmt OR a.starttimegmt IS NULL, a.endtimegmt, NULL) AS end_at_utc,
        DATETIME(a.starttimelocal) AS start_at_local,
        a.timezoneid AS source_timezone_id
    ) AS timing,
    STRUCT(
        {{ garmin_activity_number('a.distance', 0, none) }} AS distance_m,
        {{ garmin_activity_number('a.duration', 0, none) }} AS duration_s,
        {{ garmin_activity_number('a.movingduration', 0, none) }} AS moving_duration_s,
        {{ garmin_activity_number('a.elapsedduration', 0, none) }} AS elapsed_duration_s,
        {{ garmin_activity_number('a.averagespeed', 0, none) }} AS avg_speed_m_per_s,
        {{ garmin_activity_number('a.maxspeed', 0, none) }} AS max_speed_m_per_s,
        {{ garmin_activity_number('a.avggradeadjustedspeed', 0, none) }} AS avg_grade_adjusted_speed_m_per_s,
        {{ garmin_activity_number('a.elevationgain', 0, none) }} AS elevation_gain_m,
        {{ garmin_activity_number('a.elevationloss', 0, none) }} AS elevation_loss_m,
        {{ garmin_activity_number('a.calories', 0, none) }} AS calories,
        {{ garmin_activity_number('a.bmrcalories', 0, none) }} AS bmr_calories,
        {{ garmin_activity_number('a.averagehr', 0, none) }} AS avg_hr_bpm,
        {{ garmin_activity_number('a.maxhr', 0, none) }} AS max_hr_bpm,
        {{ garmin_activity_number('a.avgpower', 0, none) }} AS avg_power_w,
        {{ garmin_activity_number('a.maxpower', 0, none) }} AS max_power_w,
        {{ garmin_activity_number('a.normpower', 0, none) }} AS norm_power_w,
        {{ garmin_activity_number('a.avgrespirationrate', 0, none) }} AS avg_respiration_rpm,
        {{ garmin_activity_number('a.minrespirationrate', 0, none) }} AS min_respiration_rpm,
        {{ garmin_activity_number('a.maxrespirationrate', 0, none) }} AS max_respiration_rpm,
        {{ garmin_activity_number('a.activitytrainingload', 0, none) }} AS training_load,
        {{ garmin_activity_number('a.vo2maxvalue', 0, none) }} AS vo2max_during_activity,
        SAFE_CAST({{ garmin_activity_number('a.steps', 0, none) }} AS INT64) AS steps,
        SAFE_CAST({{ garmin_activity_number('a.lapcount', 0, none) }} AS INT64) AS lap_count,
        SAFE_CAST({{ garmin_activity_number('a.vigorousintensityminutes', 0, none) }} AS INT64)
            AS vigorous_intensity_min,
        SAFE_CAST({{ garmin_activity_number('a.moderateintensityminutes', 0, none) }} AS INT64)
            AS moderate_intensity_min,
        {{ garmin_activity_number('a.avgelevation', none, none) }} AS avg_elevation_m,
        {{ garmin_activity_number('a.minelevation', none, none) }} AS min_elevation_m,
        {{ garmin_activity_number('a.maxelevation', none, none) }} AS max_elevation_m,
        {{ garmin_activity_number('a.avgverticalspeed', none, none) }} AS avg_vertical_speed_m_per_s,
        {{ garmin_activity_number('a.maxverticalspeed', none, none) }} AS max_vertical_speed_m_per_s,
        {{ garmin_activity_number('a.mintemperature', none, none) }} AS min_temp_celsius,
        {{ garmin_activity_number('a.maxtemperature', none, none) }} AS max_temp_celsius,
        {{ garmin_activity_number('a.differencebodybattery', none, none) }} AS body_battery_delta,
        {{ garmin_activity_number('a.aerobictrainingeffect', 0, 5) }} AS aerobic_te,
        {{ garmin_activity_number('a.anaerobictrainingeffect', 0, 5) }} AS anaerobic_te,
        a.aerobictrainingeffectmessage AS aerobic_te_message,
        a.anaerobictrainingeffectmessage AS anaerobic_te_message,
        a.trainingeffectlabel AS training_effect_label
    ) AS performance,
    IF(a.sport_family = 'running', STRUCT(
        SAFE_DIVIDE(1000.0, {{ garmin_activity_number('a.averagespeed', 0, none) }} * 60.0) AS avg_pace_min_per_km,
        {{ garmin_activity_number('a.averagerunningcadenceinstepsperminute', 0, none) }} AS avg_cadence_spm,
        {{ garmin_activity_number('a.maxrunningcadenceinstepsperminute', 0, none) }} AS max_cadence_spm,
        {{ garmin_activity_number('a.avgstridelength / 100.0', 0, none) }} AS avg_stride_length_m,
        {{ garmin_activity_number('a.avggroundcontacttime', 0, none) }} AS avg_ground_contact_ms,
        {{ garmin_activity_number('a.avgverticaloscillation * 10.0', 0, none) }} AS avg_vertical_oscillation_mm,
        {{ garmin_activity_number('a.avgverticalratio', 0, 100) }} AS avg_vertical_ratio,
        STRUCT(
            {{ garmin_activity_number('a.fastestsplit_1000', 0, none) }} AS dist_1000m_s,
            {{ garmin_activity_number('a.fastestsplit_1609', 0, none) }} AS dist_1609m_s,
            {{ garmin_activity_number('a.fastestsplit_5000', 0, none) }} AS dist_5000m_s,
            {{ garmin_activity_number('a.fastestsplit_10000', 0, none) }} AS dist_10000m_s,
            {{ garmin_activity_number('a.fastestsplit_21098', 0, none) }} AS dist_21098m_s,
            {{ garmin_activity_number('a.fastestsplit_42195', 0, none) }} AS dist_42195m_s
        ) AS fastest_splits
    ), NULL) AS running,
    IF(a.sport_family = 'cycling', STRUCT(
        {{ garmin_activity_number('a.avgpower', 0, none) }} AS avg_power_w,
        {{ garmin_activity_number('a.normpower', 0, none) }} AS norm_power_w
    ), NULL) AS cycling,
    IF(a.sport_family = 'strength', STRUCT(
        SAFE_CAST({{ garmin_activity_number('a.totalreps', 0, none) }} AS INT64) AS total_reps,
        SAFE_CAST({{ garmin_activity_number('a.totalsets', 0, none) }} AS INT64) AS total_sets,
        SAFE_CAST({{ garmin_activity_number('a.activesets', 0, none) }} AS INT64) AS active_sets
    ), NULL) AS strength,
    STRUCT(
        {{ garmin_activity_number('a.startlatitude', -90, 90) }} AS start_latitude,
        {{ garmin_activity_number('a.startlongitude', -180, 180) }} AS start_longitude,
        {{ garmin_activity_number('a.endlatitude', -90, 90) }} AS end_latitude,
        {{ garmin_activity_number('a.endlongitude', -180, 180) }} AS end_longitude
    ) AS gps,
    STRUCT(
        a.pr AS is_pr,
        a.favorite AS is_favorite,
        a.manualactivity AS is_manual,
        a.elevationcorrected AS is_elevation_corrected,
        a.parent AS is_multisport_parent,
        a.haspolyline AS has_polyline,
        a.hassplits AS has_splits,
        a.hasintensityintervals AS has_intensity_intervals
    ) AS flags,
    a.event_type,
    (SELECT AS STRUCT a.* EXCEPT (sport_family, _metric_issues)) AS source_values,
    'garmin' AS source_system,
    'activities' AS source_table,
    CAST(a.activityid AS STRING) AS source_record_key,
    a._ingested_at,
    CASE
        WHEN
            a.activityid IS NULL OR ARRAY_LENGTH(a._metric_issues) > 0
            OR a.endtimegmt < a.starttimegmt THEN 'invalid'
        WHEN
            a.starttimegmt IS NULL OR a.starttimelocal IS NULL OR a.duration IS NULL
            OR a.activity_type.type_key IS NULL THEN 'partial'
        ELSE 'valid'
    END AS quality_status,
    ARRAY_CONCAT(
        a._metric_issues,
        IF(a.activityid IS NULL, ['missing_activity_id'], ARRAY<STRING>[]),
        IF(a.starttimegmt IS NULL, ['missing_start_at_utc'], ARRAY<STRING>[]),
        IF(a.starttimelocal IS NULL, ['missing_start_at_local'], ARRAY<STRING>[]),
        IF(a.duration IS NULL, ['missing_duration_s'], ARRAY<STRING>[]),
        IF(a.endtimegmt < a.starttimegmt, ['invalid_time_order'], ARRAY<STRING>[]),
        IF(a.activity_type.type_key IS NULL, ['missing_sport_type'], ARRAY<STRING>[])
    ) AS quality_issues
FROM qualified AS a
