{{ config(materialized='view') }}

-- One source item per activity/source_index, with independent family qualification.
WITH prepared AS (
    SELECT
        s.activityid AS activity_id,
        STRUCT(
            CONCAT(CAST(s.activityid AS STRING), ':split_summaries:', CAST(source_index AS STRING)) AS item_key,
            source_index AS source_index,
            p.split_type AS split_type,
            SAFE_CAST({{ garmin_activity_number('p.no_of_splits', 0, none) }} AS INT64) AS no_of_splits,
            {{ garmin_activity_number('p.distance', 0, none) }} AS distance_m,
            {{ garmin_activity_number('p.duration', 0, none) }} AS duration_s,
            {{ garmin_activity_number('p.moving_duration', 0, none) }} AS moving_duration_s,
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
            {{ garmin_activity_number('p.avg_grade_adjusted_speed', 0, none) }} AS avg_grade_adjusted_speed_m_per_s,
            {{ garmin_activity_number('p.avg_vertical_speed', none, none) }} AS avg_vertical_speed_m_per_s,
            {{ garmin_activity_number('p.avg_step_frequency', 0, none) }} AS avg_step_frequency_source,
            {{ garmin_activity_number('p.max_elevation_gain', 0, none) }} AS max_elevation_gain_m,
            {{ garmin_activity_number('p.average_elevation_gain', 0, none) }} AS avg_elevation_gain_m,
            {{ garmin_activity_number('p.max_distance', 0, none) }} AS max_distance_m,
            {{ garmin_activity_number('p.max_distance_with_precision', 0, none) }} AS max_distance_with_precision_m,
            {{ garmin_activity_number('p.average_temperature', none, none) }} AS avg_temp_celsius,
            {{ garmin_activity_number('p.max_temperature', none, none) }} AS max_temp_celsius,
            {{ garmin_activity_number('p.min_temperature', none, none) }} AS min_temp_celsius,
            p AS source_values,
            IF(ARRAY_LENGTH(qualification.issues) > 0, 'invalid', 'valid') AS quality_status,
            qualification.issues AS quality_issues
        ) AS item
    FROM {{ ref('dlk_garmin_svc__activity_splits') }} AS s,
        UNNEST(s.split_summaries) AS p WITH OFFSET AS source_index
    CROSS JOIN UNNEST([STRUCT(ARRAY_CONCAT(
        IF(
            p.no_of_splits IS NOT NULL AND {{ garmin_activity_number('p.no_of_splits', 0, none) }} IS NULL,
            ['invalid_no_of_splits'],
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
            p.moving_duration IS NOT NULL AND {{ garmin_activity_number('p.moving_duration', 0, none) }} IS NULL,
            ['invalid_moving_duration_s'],
            ARRAY<STRING>[]
        ),
        IF(
            p.elevation_gain IS NOT NULL AND {{ garmin_activity_number('p.elevation_gain', 0, none) }} IS NULL,
            ['invalid_elevation_gain_m'],
            ARRAY<STRING>[]
        ),
        IF(
            p.elevation_loss IS NOT NULL AND {{ garmin_activity_number('p.elevation_loss', 0, none) }} IS NULL,
            ['invalid_elevation_loss_m'],
            ARRAY<STRING>[]
        ),
        IF(
            p.average_speed IS NOT NULL AND {{ garmin_activity_number('p.average_speed', 0, none) }} IS NULL,
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
            p.max_run_cadence IS NOT NULL AND {{ garmin_activity_number('p.max_run_cadence', 0, none) }} IS NULL,
            ['invalid_max_run_cadence_spm'],
            ARRAY<STRING>[]
        ),
        IF(
            p.average_power IS NOT NULL AND {{ garmin_activity_number('p.average_power', 0, none) }} IS NULL,
            ['invalid_avg_power_w'],
            ARRAY<STRING>[]
        ),
        IF(
            p.max_power IS NOT NULL AND {{ garmin_activity_number('p.max_power', 0, none) }} IS NULL,
            ['invalid_max_power_w'],
            ARRAY<STRING>[]
        ),
        IF(
            p.normalized_power IS NOT NULL AND {{ garmin_activity_number('p.normalized_power', 0, none) }} IS NULL,
            ['invalid_norm_power_w'],
            ARRAY<STRING>[]
        ),
        IF(
            p.avg_grade_adjusted_speed IS NOT NULL
            AND {{ garmin_activity_number('p.avg_grade_adjusted_speed', 0, none) }} IS NULL,
            ['invalid_avg_grade_adjusted_speed_m_per_s'],
            ARRAY<STRING>[]
        ),
        IF(
            p.avg_vertical_speed IS NOT NULL
            AND {{ garmin_activity_number('p.avg_vertical_speed', none, none) }} IS NULL,
            ['invalid_avg_vertical_speed_m_per_s'],
            ARRAY<STRING>[]
        ),
        IF(
            p.avg_step_frequency IS NOT NULL AND {{ garmin_activity_number('p.avg_step_frequency', 0, none) }} IS NULL,
            ['invalid_avg_step_frequency_source'],
            ARRAY<STRING>[]
        ),
        IF(
            p.max_elevation_gain IS NOT NULL AND {{ garmin_activity_number('p.max_elevation_gain', 0, none) }} IS NULL,
            ['invalid_max_elevation_gain_m'],
            ARRAY<STRING>[]
        ),
        IF(
            p.average_elevation_gain IS NOT NULL
            AND {{ garmin_activity_number('p.average_elevation_gain', 0, none) }} IS NULL,
            ['invalid_avg_elevation_gain_m'],
            ARRAY<STRING>[]
        ),
        IF(
            p.max_distance IS NOT NULL AND {{ garmin_activity_number('p.max_distance', 0, none) }} IS NULL,
            ['invalid_max_distance_m'],
            ARRAY<STRING>[]
        ),
        IF(
            p.max_distance_with_precision IS NOT NULL
            AND {{ garmin_activity_number('p.max_distance_with_precision', 0, none) }} IS NULL,
            ['invalid_max_distance_with_precision_m'],
            ARRAY<STRING>[]
        ),
        IF(
            p.average_temperature IS NOT NULL
            AND {{ garmin_activity_number('p.average_temperature', none, none) }} IS NULL,
            ['invalid_avg_temp_celsius'],
            ARRAY<STRING>[]
        ),
        IF(
            p.max_temperature IS NOT NULL AND {{ garmin_activity_number('p.max_temperature', none, none) }} IS NULL,
            ['invalid_max_temp_celsius'],
            ARRAY<STRING>[]
        ),
        IF(
            p.min_temperature IS NOT NULL AND {{ garmin_activity_number('p.min_temperature', none, none) }} IS NULL,
            ['invalid_min_temp_celsius'],
            ARRAY<STRING>[]
        )
    ) AS issues)]) AS qualification
)

SELECT
    activity_id,
    item.item_key,
    item.source_index,
    item.split_type,
    item.no_of_splits,
    item.distance_m,
    item.duration_s,
    item.moving_duration_s,
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
    item.avg_grade_adjusted_speed_m_per_s,
    item.avg_vertical_speed_m_per_s,
    item.avg_step_frequency_source,
    item.max_elevation_gain_m,
    item.avg_elevation_gain_m,
    item.max_distance_m,
    item.max_distance_with_precision_m,
    item.avg_temp_celsius,
    item.max_temp_celsius,
    item.min_temp_celsius,
    item.source_values,
    item.quality_status,
    item.quality_issues
FROM prepared
