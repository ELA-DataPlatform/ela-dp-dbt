{{ config(materialized='view') }}

-- One source item per activity/source_index, with independent family qualification.
WITH prepared AS (
    SELECT
        s.activityid AS activity_id,
        STRUCT(
            CONCAT(CAST(s.activityid AS STRING), ':typed_splits:', CAST(source_index AS STRING)) AS item_key,
            source_index AS source_index,
            {{ garmin_health_timestamp('p.start_time_gmt') }} AS start_at_utc,
            SAFE_CAST(p.start_time_local AS DATETIME) AS start_at_local,
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
            {{ garmin_activity_number('p.average_speed', 0, none) }} AS avg_speed_m_per_s,
            {{ garmin_activity_number('p.max_speed', 0, none) }} AS max_speed_m_per_s,
            {{ garmin_activity_number('p.calories', 0, none) }} AS calories,
            {{ garmin_activity_number('p.average_hr', 0, none) }} AS avg_hr_bpm,
            {{ garmin_activity_number('p.max_hr', 0, none) }} AS max_hr_bpm,
            {{ garmin_activity_number('p.average_run_cadence', 0, none) }} AS avg_run_cadence_spm,
            {{ garmin_activity_number('p.max_run_cadence', 0, none) }} AS max_run_cadence_spm,
            {{ garmin_activity_number('p.average_power', 0, none) }} AS avg_power_w,
            {{ garmin_activity_number('p.max_power', 0, none) }} AS max_power_w,
            {{ garmin_activity_number('p.normalized_power', 0, none) }} AS norm_power_w,
            p.intensity_type AS intensity_type,
            SAFE_CAST({{ garmin_activity_number('p.message_index', 0, none) }} AS INT64) AS message_index,
            {{ garmin_activity_number('p.avg_grade_adjusted_speed', 0, none) }} AS avg_grade_adjusted_speed_m_per_s,
            {{ garmin_activity_number('p.average_temperature', none, none) }} AS avg_temp_celsius,
            {{ garmin_activity_number('p.max_temperature', none, none) }} AS max_temp_celsius,
            {{ garmin_activity_number('p.min_temperature', none, none) }} AS min_temp_celsius,
            p AS source_values,
            IF(ARRAY_LENGTH(qualification.issues) > 0, 'invalid', 'valid') AS quality_status,
            qualification.issues AS quality_issues
        ) AS item
    FROM {{ ref('dlk_garmin_svc__activity_splits') }} AS s,
        UNNEST(s.typed_splits) AS p WITH OFFSET AS source_index
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
                            p.average_speed IS NOT NULL
                            AND {{ garmin_activity_number('p.average_speed', 0, none) }} IS NULL,
                            ['invalid_avg_speed_m_per_s'],
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
                            p.normalized_power IS NOT NULL
                            AND {{ garmin_activity_number('p.normalized_power', 0, none) }} IS NULL,
                            ['invalid_norm_power_w'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.message_index IS NOT NULL
                            AND {{ garmin_activity_number('p.message_index', 0, none) }} IS NULL,
                            ['invalid_message_index'],
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
                        )
                    ),
                    IF(
                        p.start_time_gmt IS NOT NULL AND {{ garmin_health_timestamp('p.start_time_gmt') }} IS NULL,
                        ['invalid_start_at_utc'],
                        ARRAY<STRING>[]
                    ),
                    IF(
                        p.start_time_local IS NOT NULL AND SAFE_CAST(p.start_time_local AS DATETIME) IS NULL,
                        ['invalid_start_at_local'],
                        ARRAY<STRING>[]
                    )
                ) AS issues
            )
        ]
    ) AS qualification
)

SELECT
    activity_id,
    item.item_key,
    item.source_index,
    item.start_at_utc,
    item.start_at_local,
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
    item.avg_speed_m_per_s,
    item.max_speed_m_per_s,
    item.calories,
    item.avg_hr_bpm,
    item.max_hr_bpm,
    item.avg_run_cadence_spm,
    item.max_run_cadence_spm,
    item.avg_power_w,
    item.max_power_w,
    item.norm_power_w,
    item.intensity_type,
    item.message_index,
    item.avg_grade_adjusted_speed_m_per_s,
    item.avg_temp_celsius,
    item.max_temp_celsius,
    item.min_temp_celsius,
    item.source_values,
    item.quality_status,
    item.quality_issues
FROM prepared
