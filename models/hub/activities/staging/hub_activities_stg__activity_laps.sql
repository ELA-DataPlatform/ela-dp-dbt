{{ config(materialized='view') }}

-- One source item per activity/source_index, with independent family qualification.
WITH prepared AS (
    SELECT
        s.activityid AS activity_id,
        STRUCT(
            CONCAT(CAST(s.activityid AS STRING), ':laps:', CAST(source_index AS STRING)) AS item_key,
            source_index AS source_index,
            {{ garmin_health_timestamp('p.start_time_gmt') }} AS start_at_utc,
            {{ garmin_activity_number('p.start_latitude', -90, 90) }} AS start_latitude,
            {{ garmin_activity_number('p.start_longitude', -180, 180) }} AS start_longitude,
            {{ garmin_activity_number('p.end_latitude', -90, 90) }} AS end_latitude,
            {{ garmin_activity_number('p.end_longitude', -180, 180) }} AS end_longitude,
            {{ garmin_activity_number('p.distance', 0, none) }} AS distance_m,
            {{ garmin_activity_number('p.duration', 0, none) }} AS duration_s,
            {{ garmin_activity_number('p.moving_duration', 0, none) }} AS moving_duration_s,
            {{ garmin_activity_number('p.elapsed_duration', 0, none) }} AS elapsed_duration_s,
            {{ garmin_activity_number('p.elevation_gain', 0, none) }} AS elevation_gain_m,
            {{ garmin_activity_number('p.elevation_loss', 0, none) }} AS elevation_loss_m,
            {{ garmin_activity_number('p.max_elevation', none, none) }} AS max_elevation_m,
            {{ garmin_activity_number('p.min_elevation', none, none) }} AS min_elevation_m,
            {{ garmin_activity_number('p.average_speed', 0, none) }} AS avg_speed_m_per_s,
            {{ garmin_activity_number('p.average_moving_speed', 0, none) }} AS avg_moving_speed_m_per_s,
            {{ garmin_activity_number('p.max_speed', 0, none) }} AS max_speed_m_per_s,
            {{ garmin_activity_number('p.calories', 0, none) }} AS calories,
            {{ garmin_activity_number('p.bmr_calories', 0, none) }} AS bmr_calories,
            {{ garmin_activity_number('p.average_hr', 0, none) }} AS avg_hr_bpm,
            {{ garmin_activity_number('p.max_hr', 0, none) }} AS max_hr_bpm,
            {{ garmin_activity_number('p.average_run_cadence', 0, none) }} AS avg_run_cadence_spm,
            {{ garmin_activity_number('p.max_run_cadence', 0, none) }} AS max_run_cadence_spm,
            {{ garmin_activity_number('p.average_power', 0, none) }} AS avg_power_w,
            {{ garmin_activity_number('p.max_power', 0, none) }} AS max_power_w,
            {{ garmin_activity_number('p.min_power', 0, none) }} AS min_power_w,
            {{ garmin_activity_number('p.normalized_power', 0, none) }} AS norm_power_w,
            CASE '{{ var('garmin_activity_total_work_unit', 'unknown') }}'
                WHEN 'j' THEN {{ garmin_activity_number('p.total_work', 0, none) }} / 1000.0
                WHEN 'kj' THEN {{ garmin_activity_number('p.total_work', 0, none) }}
                WHEN 'kcal' THEN {{ garmin_activity_number('p.total_work', 0, none) }} * 4.184
            END AS total_work_kj,
            '{{ var('garmin_activity_total_work_unit', 'unknown') }}' AS source_work_unit,
            {{ garmin_activity_number('p.ground_contact_time', 0, none) }} AS ground_contact_ms,
            {{ garmin_activity_number('p.stride_length', 0, none) }} / 100.0 AS stride_length_m,
            {{ garmin_activity_number('p.vertical_oscillation', 0, none) }} * 10.0 AS vertical_oscillation_mm,
            {{ garmin_activity_number('p.vertical_ratio', 0, 100) }} AS vertical_ratio_pct,
            {{ garmin_activity_number('p.avg_grade_adjusted_speed', 0, none) }} AS avg_grade_adjusted_speed_m_per_s,
            {{ garmin_activity_number('p.average_temperature', none, none) }} AS avg_temp_celsius,
            {{ garmin_activity_number('p.max_temperature', none, none) }} AS max_temp_celsius,
            {{ garmin_activity_number('p.min_temperature', none, none) }} AS min_temp_celsius,
            p.intensity_type AS intensity_type,
            SAFE_CAST({{ garmin_activity_number('p.lap_index', 0, none) }} AS INT64) AS lap_index,
            SAFE_CAST({{ garmin_activity_number('p.message_index', 0, none) }} AS INT64) AS message_index,
            {{ garmin_activity_number('p.max_vertical_speed', none, none) }} AS max_vertical_speed_m_per_s,
            SAFE_CAST({{ garmin_activity_number('p.direct_workout_compliance_score', 0, none) }} AS INT64)
                AS direct_workout_compliance_score,
            p AS source_values,
            CASE
                WHEN ARRAY_LENGTH(qualification.issues) > 0 THEN 'invalid'
                WHEN
                    p.total_work IS NOT NULL
                    AND '{{ var('garmin_activity_total_work_unit', 'unknown') }}' NOT IN ('j', 'kj', 'kcal')
                    THEN 'partial'
                ELSE 'valid'
            END AS quality_status,
            ARRAY_CONCAT(
                qualification.issues,
                IF(
                    p.total_work IS NOT NULL
                    AND '{{ var('garmin_activity_total_work_unit', 'unknown') }}' NOT IN ('j', 'kj', 'kcal'),
                    ['unverified_total_work_unit'], ARRAY<STRING>[]
                )
            ) AS quality_issues
        ) AS item
    FROM {{ ref('dlk_garmin_svc__activity_splits') }} AS s,
        UNNEST(s.laps) AS p WITH OFFSET AS source_index
    CROSS JOIN UNNEST(
        [
            STRUCT(
                ARRAY_CONCAT(
                    ARRAY_CONCAT(
                        IF(
                            p.start_latitude IS NOT NULL
                            AND {{ garmin_activity_number('p.start_latitude', -90, 90) }} IS NULL,
                            ['invalid_start_latitude'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.start_longitude IS NOT NULL
                            AND {{ garmin_activity_number('p.start_longitude', -180, 180) }} IS NULL,
                            ['invalid_start_longitude'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.end_latitude IS NOT NULL
                            AND {{ garmin_activity_number('p.end_latitude', -90, 90) }} IS NULL,
                            ['invalid_end_latitude'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.end_longitude IS NOT NULL
                            AND {{ garmin_activity_number('p.end_longitude', -180, 180) }} IS NULL,
                            ['invalid_end_longitude'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.distance IS NOT NULL AND {{ garmin_activity_number('p.distance', 0, none) }} IS NULL,
                            ['invalid_distance_m'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.duration IS NOT NULL AND {{ garmin_activity_number('p.duration', 0, none) }} IS NULL,
                            ['invalid_duration_s'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.moving_duration IS NOT NULL
                            AND {{ garmin_activity_number('p.moving_duration', 0, none) }} IS NULL,
                            ['invalid_moving_duration_s'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.elapsed_duration IS NOT NULL
                            AND {{ garmin_activity_number('p.elapsed_duration', 0, none) }} IS NULL,
                            ['invalid_elapsed_duration_s'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.elevation_gain IS NOT NULL
                            AND {{ garmin_activity_number('p.elevation_gain', 0, none) }} IS NULL,
                            ['invalid_elevation_gain_m'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.elevation_loss IS NOT NULL
                            AND {{ garmin_activity_number('p.elevation_loss', 0, none) }} IS NULL,
                            ['invalid_elevation_loss_m'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.max_elevation IS NOT NULL
                            AND {{ garmin_activity_number('p.max_elevation', none, none) }} IS NULL,
                            ['invalid_max_elevation_m'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.min_elevation IS NOT NULL
                            AND {{ garmin_activity_number('p.min_elevation', none, none) }} IS NULL,
                            ['invalid_min_elevation_m'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.average_speed IS NOT NULL
                            AND {{ garmin_activity_number('p.average_speed', 0, none) }} IS NULL,
                            ['invalid_avg_speed_m_per_s'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.average_moving_speed IS NOT NULL
                            AND {{ garmin_activity_number('p.average_moving_speed', 0, none) }} IS NULL,
                            ['invalid_avg_moving_speed_m_per_s'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.max_speed IS NOT NULL AND {{ garmin_activity_number('p.max_speed', 0, none) }} IS NULL,
                            ['invalid_max_speed_m_per_s'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.calories IS NOT NULL AND {{ garmin_activity_number('p.calories', 0, none) }} IS NULL,
                            ['invalid_calories'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.bmr_calories IS NOT NULL
                            AND {{ garmin_activity_number('p.bmr_calories', 0, none) }} IS NULL,
                            ['invalid_bmr_calories'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.average_hr IS NOT NULL AND {{ garmin_activity_number('p.average_hr', 0, none) }} IS NULL,
                            ['invalid_avg_hr_bpm'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.max_hr IS NOT NULL AND {{ garmin_activity_number('p.max_hr', 0, none) }} IS NULL,
                            ['invalid_max_hr_bpm'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.average_run_cadence IS NOT NULL
                            AND {{ garmin_activity_number('p.average_run_cadence', 0, none) }} IS NULL,
                            ['invalid_avg_run_cadence_spm'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.max_run_cadence IS NOT NULL
                            AND {{ garmin_activity_number('p.max_run_cadence', 0, none) }} IS NULL,
                            ['invalid_max_run_cadence_spm'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.average_power IS NOT NULL
                            AND {{ garmin_activity_number('p.average_power', 0, none) }} IS NULL,
                            ['invalid_avg_power_w'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.max_power IS NOT NULL AND {{ garmin_activity_number('p.max_power', 0, none) }} IS NULL,
                            ['invalid_max_power_w'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.min_power IS NOT NULL AND {{ garmin_activity_number('p.min_power', 0, none) }} IS NULL,
                            ['invalid_min_power_w'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.normalized_power IS NOT NULL
                            AND {{ garmin_activity_number('p.normalized_power', 0, none) }} IS NULL,
                            ['invalid_norm_power_w'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.total_work IS NOT NULL AND {{ garmin_activity_number('p.total_work', 0, none) }} IS NULL,
                            ['invalid_total_work_kj'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.ground_contact_time IS NOT NULL
                            AND {{ garmin_activity_number('p.ground_contact_time', 0, none) }} IS NULL,
                            ['invalid_ground_contact_ms'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.stride_length IS NOT NULL
                            AND {{ garmin_activity_number('p.stride_length', 0, none) }} IS NULL,
                            ['invalid_stride_length_m'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.vertical_oscillation IS NOT NULL
                            AND {{ garmin_activity_number('p.vertical_oscillation', 0, none) }} IS NULL,
                            ['invalid_vertical_oscillation_mm'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.vertical_ratio IS NOT NULL
                            AND {{ garmin_activity_number('p.vertical_ratio', 0, 100) }} IS NULL,
                            ['invalid_vertical_ratio_pct'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.avg_grade_adjusted_speed IS NOT NULL
                            AND {{ garmin_activity_number('p.avg_grade_adjusted_speed', 0, none) }} IS NULL,
                            ['invalid_avg_grade_adjusted_speed_m_per_s'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.average_temperature IS NOT NULL
                            AND {{ garmin_activity_number('p.average_temperature', none, none) }} IS NULL,
                            ['invalid_avg_temp_celsius'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.max_temperature IS NOT NULL
                            AND {{ garmin_activity_number('p.max_temperature', none, none) }} IS NULL,
                            ['invalid_max_temp_celsius'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.min_temperature IS NOT NULL
                            AND {{ garmin_activity_number('p.min_temperature', none, none) }} IS NULL,
                            ['invalid_min_temp_celsius'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.lap_index IS NOT NULL AND {{ garmin_activity_number('p.lap_index', 0, none) }} IS NULL,
                            ['invalid_lap_index'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.message_index IS NOT NULL
                            AND {{ garmin_activity_number('p.message_index', 0, none) }} IS NULL,
                            ['invalid_message_index'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.max_vertical_speed IS NOT NULL
                            AND {{ garmin_activity_number('p.max_vertical_speed', none, none) }} IS NULL,
                            ['invalid_max_vertical_speed_m_per_s'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.direct_workout_compliance_score IS NOT NULL
                            AND {{ garmin_activity_number('p.direct_workout_compliance_score', 0, none) }} IS NULL,
                            ['invalid_direct_workout_compliance_score'],
                            ARRAY<STRING>[]
                        )
                    ),
                    IF(
                        p.start_time_gmt IS NOT NULL AND {{ garmin_health_timestamp('p.start_time_gmt') }} IS NULL,
                        ['invalid_start_at_utc'],
                        ARRAY<STRING>[]
                    )
                )
                    AS issues
            )
        ]
    )
        AS qualification
)

SELECT
    activity_id,
    item.item_key,
    item.source_index,
    item.start_at_utc,
    item.start_latitude,
    item.start_longitude,
    item.end_latitude,
    item.end_longitude,
    item.distance_m,
    item.duration_s,
    item.moving_duration_s,
    item.elapsed_duration_s,
    item.elevation_gain_m,
    item.elevation_loss_m,
    item.max_elevation_m,
    item.min_elevation_m,
    item.avg_speed_m_per_s,
    item.avg_moving_speed_m_per_s,
    item.max_speed_m_per_s,
    item.calories,
    item.bmr_calories,
    item.avg_hr_bpm,
    item.max_hr_bpm,
    item.avg_run_cadence_spm,
    item.max_run_cadence_spm,
    item.avg_power_w,
    item.max_power_w,
    item.min_power_w,
    item.norm_power_w,
    item.total_work_kj,
    item.source_work_unit,
    item.ground_contact_ms,
    item.stride_length_m,
    item.vertical_oscillation_mm,
    item.vertical_ratio_pct,
    item.avg_grade_adjusted_speed_m_per_s,
    item.avg_temp_celsius,
    item.max_temp_celsius,
    item.min_temp_celsius,
    item.intensity_type,
    item.lap_index,
    item.message_index,
    item.max_vertical_speed_m_per_s,
    item.direct_workout_compliance_score,
    item.source_values,
    item.quality_status,
    item.quality_issues
FROM prepared
