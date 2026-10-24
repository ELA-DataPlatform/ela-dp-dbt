{{ config(materialized='view') }}

-- One source sample, keyed by parent activity and source offset, never by timestamp.
WITH points AS (
    SELECT
        d.activityid AS activity_id,
        s.sample_index,
        d._ingested_at,
        ARRAY(
            SELECT AS STRUCT
                v.metric_index,
                v.metric_value,
                v.source_text,
                md.metric_key,
                md.unit_key,
                md.unit_factor
            FROM UNNEST(s.metric_values) AS v
            LEFT JOIN UNNEST(d.metric_descriptors) AS md ON v.metric_index = md.metrics_index
            ORDER BY v.metric_index, md.metric_key
        ) AS metric_values
    FROM {{ ref('dlk_garmin_svc__activity_details') }} AS d,
        UNNEST(d.indexed_metric_samples) AS s
),

raw_values AS (
    SELECT
        p.*,
        (SELECT MAX(IF(
            v.metric_key = 'directTimestamp' AND v.unit_key = 'gmt',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_timestamp_gmt,
        (SELECT MAX(IF(
            v.metric_key = 'sumElapsedDuration' AND v.unit_key = 'second',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_elapsed_s,
        (SELECT MAX(IF(
            v.metric_key = 'sumDuration' AND v.unit_key = 'second',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_duration_s,
        (SELECT MAX(IF(
            v.metric_key = 'sumMovingDuration' AND v.unit_key = 'second',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_moving_duration_s,
        (SELECT MAX(IF(
            v.metric_key = 'sumDistance' AND v.unit_key = 'meter',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_cum_distance_m,
        (SELECT MAX(IF(
            v.metric_key = 'directSpeed' AND v.unit_key = 'mps',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_speed_m_per_s,
        (SELECT MAX(IF(
            v.metric_key = 'directGradeAdjustedSpeed' AND v.unit_key = 'mps',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_grade_adj_speed_m_per_s,
        (SELECT MAX(IF(
            v.metric_key = 'directHeartRate' AND v.unit_key = 'bpm',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_heart_rate_bpm,
        (SELECT MAX(IF(
            v.metric_key = 'directBodyBattery' AND v.unit_key = 'dimensionless',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_body_battery,
        (SELECT MAX(IF(
            v.metric_key = 'directRespirationRate' AND v.unit_key = 'breathsPerMinute',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_respiration_rpm,
        (SELECT MAX(IF(
            v.metric_key = 'directPerformanceCondition' AND v.unit_key = 'dimensionless',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_performance_condition,
        (SELECT MAX(IF(
            v.metric_key = 'directDoubleCadence' AND v.unit_key = 'stepsPerMinute',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_cadence_spm,
        (SELECT MAX(IF(
            v.metric_key = 'directFractionalCadence' AND v.unit_key = 'stepsPerMinute',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_fractional_cadence,
        (SELECT MAX(IF(
            v.metric_key = 'directRunCadence' AND v.unit_key = 'stepsPerMinute',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_run_cadence_spm,
        (SELECT MAX(IF(
            v.metric_key = 'directGroundContactTime' AND v.unit_key = 'ms',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_ground_contact_ms,
        (SELECT MAX(IF(
            v.metric_key = 'directStrideLength' AND v.unit_key = 'centimeter',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_stride_length_m,
        (SELECT MAX(IF(
            v.metric_key = 'directVerticalOscillation' AND v.unit_key = 'centimeter',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_vertical_oscillation_mm,
        (SELECT MAX(IF(
            v.metric_key = 'directVerticalRatio' AND v.unit_key = 'dimensionless',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_vertical_ratio,
        (SELECT MAX(IF(
            v.metric_key = 'directVerticalSpeed' AND v.unit_key = 'mps',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_vertical_speed_m_per_s,
        (SELECT MAX(IF(
            v.metric_key = 'directLatitude' AND v.unit_key = 'dd',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_latitude,
        (SELECT MAX(IF(
            v.metric_key = 'directLongitude' AND v.unit_key = 'dd',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_longitude,
        (SELECT MAX(IF(
            v.metric_key = 'directElevation' AND v.unit_key = 'meter',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_elevation_m,
        (SELECT MAX(IF(
            v.metric_key = 'directCorrectedElevation' AND v.unit_key = 'meter',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_corrected_elevation_m,
        (SELECT MAX(IF(
            v.metric_key = 'directPower' AND v.unit_key = 'watt',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_power_w,
        (SELECT MAX(IF(
            v.metric_key = 'sumAccumulatedPower' AND v.unit_key = 'watt',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_accumulated_power_w,
        (SELECT MAX(IF(
            v.metric_key = 'directAirTemperature' AND v.unit_key = 'celcius',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_air_temp_celsius,
        (SELECT MAX(IF(
            v.metric_key = 'directAvailableStamina' AND v.unit_key = 'dimensionless',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_stamina_available,
        (SELECT MAX(IF(
            v.metric_key = 'directPotentialStamina' AND v.unit_key = 'dimensionless',
            v.metric_value, NULL
        )) FROM UNNEST(p.metric_values) AS v) AS raw_stamina_potential
    FROM points AS p
),

parsed AS (
    SELECT
        activity_id,
        sample_index,
        metric_values AS source_values,
        _ingested_at,
        SAFE.TIMESTAMP_MILLIS(SAFE_CAST(raw_timestamp_gmt AS INT64)) AS timestamp_gmt,
        {{ garmin_activity_number('raw_elapsed_s', 0, none) }} AS elapsed_s,
        {{ garmin_activity_number('raw_duration_s', 0, none) }} AS duration_s,
        {{ garmin_activity_number('raw_moving_duration_s', 0, none) }} AS moving_duration_s,
        {{ garmin_activity_number('raw_cum_distance_m', 0, none) }} AS cum_distance_m,
        {{ garmin_activity_number('raw_speed_m_per_s', 0, none) }} AS speed_m_per_s,
        {{ garmin_activity_number('raw_grade_adj_speed_m_per_s', 0, none) }} AS grade_adj_speed_m_per_s,
        {{ garmin_activity_number('raw_heart_rate_bpm', 1, none) }} AS heart_rate_bpm,
        {{ garmin_activity_number('raw_body_battery', 0, 100) }} AS body_battery,
        {{ garmin_activity_number('raw_respiration_rpm', 0, none) }} AS respiration_rpm,
        {{ garmin_activity_number('raw_performance_condition', none, none) }} AS performance_condition,
        {{ garmin_activity_number('raw_cadence_spm', 0, none) }} AS cadence_spm,
        {{ garmin_activity_number('raw_fractional_cadence', 0, none) }} AS fractional_cadence,
        {{ garmin_activity_number('raw_run_cadence_spm', 0, none) }} AS run_cadence_spm,
        {{ garmin_activity_number('raw_ground_contact_ms', 0, none) }} AS ground_contact_ms,
        {{ garmin_activity_number('raw_stride_length_m', 0, none) }} * 0.01 AS stride_length_m,
        {{ garmin_activity_number('raw_vertical_oscillation_mm', 0, none) }} * 10 AS vertical_oscillation_mm,
        {{ garmin_activity_number('raw_vertical_ratio', 0, 100) }} AS vertical_ratio,
        {{ garmin_activity_number('raw_vertical_speed_m_per_s', none, none) }} AS vertical_speed_m_per_s,
        {{ garmin_activity_number('raw_latitude', -90, 90) }} AS latitude,
        {{ garmin_activity_number('raw_longitude', -180, 180) }} AS longitude,
        {{ garmin_activity_number('raw_elevation_m', none, none) }} AS elevation_m,
        {{ garmin_activity_number('raw_corrected_elevation_m', none, none) }} AS corrected_elevation_m,
        {{ garmin_activity_number('raw_power_w', 0, none) }} AS power_w,
        {{ garmin_activity_number('raw_accumulated_power_w', 0, none) }} AS accumulated_power_w,
        {{ garmin_activity_number('raw_air_temp_celsius', none, none) }} AS air_temp_celsius,
        {{ garmin_activity_number('raw_stamina_available', 0, 100) }} AS stamina_available,
        {{ garmin_activity_number('raw_stamina_potential', 0, 100) }} AS stamina_potential,
        ARRAY_CONCAT(
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'directTimestamp' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'gmt'
            ), ['unexpected_unit_timestamp_gmt'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'sumElapsedDuration' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'second'
            ), ['unexpected_unit_elapsed_s'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'sumDuration' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'second'
            ), ['unexpected_unit_duration_s'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'sumMovingDuration' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'second'
            ), ['unexpected_unit_moving_duration_s'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'sumDistance' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'meter'
            ), ['unexpected_unit_cum_distance_m'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'directSpeed' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'mps'
            ), ['unexpected_unit_speed_m_per_s'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'directGradeAdjustedSpeed' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'mps'
            ), ['unexpected_unit_grade_adj_speed_m_per_s'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'directHeartRate' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'bpm'
            ), ['unexpected_unit_heart_rate_bpm'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'directBodyBattery' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'dimensionless'
            ), ['unexpected_unit_body_battery'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'directRespirationRate' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'breathsPerMinute'
            ), ['unexpected_unit_respiration_rpm'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'directPerformanceCondition' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'dimensionless'
            ), ['unexpected_unit_performance_condition'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'directDoubleCadence' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'stepsPerMinute'
            ), ['unexpected_unit_cadence_spm'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'directFractionalCadence' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'stepsPerMinute'
            ), ['unexpected_unit_fractional_cadence'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'directRunCadence' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'stepsPerMinute'
            ), ['unexpected_unit_run_cadence_spm'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'directGroundContactTime' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'ms'
            ), ['unexpected_unit_ground_contact_ms'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'directStrideLength' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'centimeter'
            ), ['unexpected_unit_stride_length_m'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'directVerticalOscillation' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'centimeter'
            ), ['unexpected_unit_vertical_oscillation_mm'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'directVerticalRatio' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'dimensionless'
            ), ['unexpected_unit_vertical_ratio'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'directVerticalSpeed' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'mps'
            ), ['unexpected_unit_vertical_speed_m_per_s'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'directLatitude' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'dd'
            ), ['unexpected_unit_latitude'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'directLongitude' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'dd'
            ), ['unexpected_unit_longitude'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'directElevation' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'meter'
            ), ['unexpected_unit_elevation_m'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'directCorrectedElevation' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'meter'
            ), ['unexpected_unit_corrected_elevation_m'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'directPower' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'watt'
            ), ['unexpected_unit_power_w'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'sumAccumulatedPower' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'watt'
            ), ['unexpected_unit_accumulated_power_w'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'directAirTemperature' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'celcius'
            ), ['unexpected_unit_air_temp_celsius'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'directAvailableStamina' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'dimensionless'
            ), ['unexpected_unit_stamina_available'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE
                    v.metric_key = 'directPotentialStamina' AND v.metric_value IS NOT NULL
                    AND v.unit_key IS DISTINCT FROM 'dimensionless'
            ), ['unexpected_unit_stamina_potential'], ARRAY<STRING>[]),
            IF(
                raw_timestamp_gmt IS NOT NULL AND {{ garmin_activity_number('raw_timestamp_gmt', none, none) }} IS NULL,
                ['invalid_timestamp_gmt'], ARRAY<STRING>[]
            ),
            IF(
                raw_elapsed_s IS NOT NULL AND {{ garmin_activity_number('raw_elapsed_s', 0, none) }} IS NULL,
                ['invalid_elapsed_s'], ARRAY<STRING>[]
            ),
            IF(
                raw_duration_s IS NOT NULL AND {{ garmin_activity_number('raw_duration_s', 0, none) }} IS NULL,
                ['invalid_duration_s'], ARRAY<STRING>[]
            ),
            IF(
                raw_moving_duration_s IS NOT NULL
                AND {{ garmin_activity_number('raw_moving_duration_s', 0, none) }} IS NULL,
                ['invalid_moving_duration_s'], ARRAY<STRING>[]
            ),
            IF(
                raw_cum_distance_m IS NOT NULL AND {{ garmin_activity_number('raw_cum_distance_m', 0, none) }} IS NULL,
                ['invalid_cum_distance_m'], ARRAY<STRING>[]
            ),
            IF(
                raw_speed_m_per_s IS NOT NULL AND {{ garmin_activity_number('raw_speed_m_per_s', 0, none) }} IS NULL,
                ['invalid_speed_m_per_s'], ARRAY<STRING>[]
            ),
            IF(
                raw_grade_adj_speed_m_per_s IS NOT NULL
                AND {{ garmin_activity_number('raw_grade_adj_speed_m_per_s', 0, none) }} IS NULL,
                ['invalid_grade_adj_speed_m_per_s'], ARRAY<STRING>[]
            ),
            IF(
                raw_heart_rate_bpm IS NOT NULL AND {{ garmin_activity_number('raw_heart_rate_bpm', 1, none) }} IS NULL,
                ['invalid_heart_rate_bpm'], ARRAY<STRING>[]
            ),
            IF(
                raw_body_battery IS NOT NULL AND {{ garmin_activity_number('raw_body_battery', 0, 100) }} IS NULL,
                ['invalid_body_battery'], ARRAY<STRING>[]
            ),
            IF(
                raw_respiration_rpm IS NOT NULL
                AND {{ garmin_activity_number('raw_respiration_rpm', 0, none) }} IS NULL,
                ['invalid_respiration_rpm'], ARRAY<STRING>[]
            ),
            IF(
                raw_performance_condition IS NOT NULL
                AND {{ garmin_activity_number('raw_performance_condition', none, none) }} IS NULL,
                ['invalid_performance_condition'], ARRAY<STRING>[]
            ),
            IF(
                raw_cadence_spm IS NOT NULL AND {{ garmin_activity_number('raw_cadence_spm', 0, none) }} IS NULL,
                ['invalid_cadence_spm'], ARRAY<STRING>[]
            ),
            IF(
                raw_fractional_cadence IS NOT NULL
                AND {{ garmin_activity_number('raw_fractional_cadence', 0, none) }} IS NULL,
                ['invalid_fractional_cadence'], ARRAY<STRING>[]
            ),
            IF(
                raw_run_cadence_spm IS NOT NULL
                AND {{ garmin_activity_number('raw_run_cadence_spm', 0, none) }} IS NULL,
                ['invalid_run_cadence_spm'], ARRAY<STRING>[]
            ),
            IF(
                raw_ground_contact_ms IS NOT NULL
                AND {{ garmin_activity_number('raw_ground_contact_ms', 0, none) }} IS NULL,
                ['invalid_ground_contact_ms'], ARRAY<STRING>[]
            ),
            IF(
                raw_stride_length_m IS NOT NULL
                AND {{ garmin_activity_number('raw_stride_length_m', 0, none) }} IS NULL,
                ['invalid_stride_length_m'], ARRAY<STRING>[]
            ),
            IF(
                raw_vertical_oscillation_mm IS NOT NULL
                AND {{ garmin_activity_number('raw_vertical_oscillation_mm', 0, none) }} IS NULL,
                ['invalid_vertical_oscillation_mm'], ARRAY<STRING>[]
            ),
            IF(
                raw_vertical_ratio IS NOT NULL AND {{ garmin_activity_number('raw_vertical_ratio', 0, 100) }} IS NULL,
                ['invalid_vertical_ratio'], ARRAY<STRING>[]
            ),
            IF(
                raw_vertical_speed_m_per_s IS NOT NULL
                AND {{ garmin_activity_number('raw_vertical_speed_m_per_s', none, none) }} IS NULL,
                ['invalid_vertical_speed_m_per_s'], ARRAY<STRING>[]
            ),
            IF(
                raw_latitude IS NOT NULL AND {{ garmin_activity_number('raw_latitude', -90, 90) }} IS NULL,
                ['invalid_latitude'], ARRAY<STRING>[]
            ),
            IF(
                raw_longitude IS NOT NULL AND {{ garmin_activity_number('raw_longitude', -180, 180) }} IS NULL,
                ['invalid_longitude'], ARRAY<STRING>[]
            ),
            IF(
                raw_elevation_m IS NOT NULL AND {{ garmin_activity_number('raw_elevation_m', none, none) }} IS NULL,
                ['invalid_elevation_m'], ARRAY<STRING>[]
            ),
            IF(
                raw_corrected_elevation_m IS NOT NULL
                AND {{ garmin_activity_number('raw_corrected_elevation_m', none, none) }} IS NULL,
                ['invalid_corrected_elevation_m'], ARRAY<STRING>[]
            ),
            IF(
                raw_power_w IS NOT NULL AND {{ garmin_activity_number('raw_power_w', 0, none) }} IS NULL,
                ['invalid_power_w'], ARRAY<STRING>[]
            ),
            IF(
                raw_accumulated_power_w IS NOT NULL
                AND {{ garmin_activity_number('raw_accumulated_power_w', 0, none) }} IS NULL,
                ['invalid_accumulated_power_w'], ARRAY<STRING>[]
            ),
            IF(
                raw_air_temp_celsius IS NOT NULL
                AND {{ garmin_activity_number('raw_air_temp_celsius', none, none) }} IS NULL,
                ['invalid_air_temp_celsius'], ARRAY<STRING>[]
            ),
            IF(
                raw_stamina_available IS NOT NULL
                AND {{ garmin_activity_number('raw_stamina_available', 0, 100) }} IS NULL,
                ['invalid_stamina_available'], ARRAY<STRING>[]
            ),
            IF(
                raw_stamina_potential IS NOT NULL
                AND {{ garmin_activity_number('raw_stamina_potential', 0, 100) }} IS NULL,
                ['invalid_stamina_potential'], ARRAY<STRING>[]
            ),
            IF(
                EXISTS (
                    SELECT 1 FROM UNNEST(metric_values) AS v
                    WHERE v.metric_key IS NULL
                ),
                ['missing_metric_descriptor'], ARRAY<STRING>[]
            ),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                GROUP BY v.metric_index
                HAVING COUNT(*) > 1
            ),
            ['duplicate_metric_descriptor'], ARRAY<STRING>[]),
            IF(EXISTS (
                SELECT 1 FROM UNNEST(metric_values) AS v
                WHERE v.source_text != 'null' AND v.metric_value IS NULL
            ),
            ['invalid_numeric_slot'], ARRAY<STRING>[])
        ) AS _invalid_issues
    FROM raw_values
)

SELECT
    * EXCEPT (_invalid_issues),
    CONCAT(CAST(activity_id AS STRING), ':', CAST(sample_index AS STRING)) AS sample_key,
    CASE
        WHEN timestamp_gmt IS NULL OR ARRAY_LENGTH(_invalid_issues) > 0 THEN 'invalid'
        WHEN NOT EXISTS (
            SELECT 1 FROM UNNEST(source_values) AS v
            WHERE v.metric_key != 'directTimestamp' AND v.metric_value IS NOT NULL
        ) THEN 'partial'
        ELSE 'valid'
    END AS quality_status,
    ARRAY_CONCAT(
        _invalid_issues,
        IF(timestamp_gmt IS NULL, ['invalid_timestamp'], ARRAY<STRING>[])
    ) AS quality_issues
FROM parsed
