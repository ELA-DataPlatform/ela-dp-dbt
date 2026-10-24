{{ config(materialized='view') }}

{% set temperature_unit = var('garmin_activity_weather_temperature_unit', 'fahrenheit') %}
{% set wind_unit = var('garmin_activity_weather_wind_unit', 'unknown') %}
{% if execute and temperature_unit not in ['fahrenheit', 'celsius', 'unknown'] %}
    {% do exceptions.raise_compiler_error('Invalid Garmin activity weather temperature unit') %}
{% endif %}
{% if execute and wind_unit not in ['km_h', 'mph', 'm_s', 'unknown'] %}
    {% do exceptions.raise_compiler_error('Invalid Garmin activity weather wind unit') %}
{% endif %}

WITH normalized AS (
    SELECT
        w.activityid AS activity_id,
        w AS source_values,
        w._ingested_at,
        {{ garmin_health_timestamp('w.weather_issue_date') }} AS observed_at_utc,
        CASE '{{ temperature_unit }}'
            WHEN 'fahrenheit' THEN ({{ garmin_activity_number('w.weather_temp') }} - 32.0) * 5.0 / 9.0
            WHEN 'celsius' THEN {{ garmin_activity_number('w.weather_temp') }}
        END AS temp_celsius,
        CASE '{{ temperature_unit }}'
            WHEN 'fahrenheit' THEN ({{ garmin_activity_number('w.weather_apparent_temp') }} - 32.0) * 5.0 / 9.0
            WHEN 'celsius' THEN {{ garmin_activity_number('w.weather_apparent_temp') }}
        END AS apparent_temp_celsius,
        CASE '{{ temperature_unit }}'
            WHEN 'fahrenheit' THEN ({{ garmin_activity_number('w.weather_dew_point') }} - 32.0) * 5.0 / 9.0
            WHEN 'celsius' THEN {{ garmin_activity_number('w.weather_dew_point') }}
        END AS dew_point_celsius,
        CASE '{{ wind_unit }}'
            WHEN 'km_h' THEN {{ garmin_activity_number('w.weather_wind_speed', 0) }}
            WHEN 'mph' THEN {{ garmin_activity_number('w.weather_wind_speed', 0) }} * 1.609344
            WHEN 'm_s' THEN {{ garmin_activity_number('w.weather_wind_speed', 0) }} * 3.6
        END AS wind_speed_kph,
        {{ garmin_activity_number('w.weather_relative_humidity', 0, 100) }} AS humidity_pct,
        {{ garmin_activity_number('w.weather_wind_direction', 0, 360) }} AS wind_direction_deg,
        {{ garmin_activity_number('w.weather_latitude', -90, 90) }} AS latitude,
        {{ garmin_activity_number('w.weather_longitude', -180, 180) }} AS longitude,
        w.weather_wind_direction_compass AS wind_direction_compass,
        w.weather_type,
        w.weather_station_id AS station_id,
        w.weather_station_name AS station_name,
        '{{ temperature_unit }}' AS source_temperature_unit,
        '{{ wind_unit }}' AS source_wind_unit
    FROM {{ ref('dlk_garmin_svc__activity_weather') }} AS w
),

qualified AS (
    SELECT
        n.*,
        ARRAY_CONCAT(
            IF(activity_id IS NULL, ['missing_activity_id'], ARRAY<STRING>[]),
            IF(
                source_values.weather_relative_humidity IS NOT NULL AND humidity_pct IS NULL,
                ['invalid_humidity_pct'], ARRAY<STRING>[]
            ),
            IF(
                source_values.weather_wind_direction IS NOT NULL AND wind_direction_deg IS NULL,
                ['invalid_wind_direction_deg'], ARRAY<STRING>[]
            ),
            IF(
                source_values.weather_latitude IS NOT NULL AND latitude IS NULL,
                ['invalid_latitude'], ARRAY<STRING>[]
            ),
            IF(
                source_values.weather_longitude IS NOT NULL AND longitude IS NULL,
                ['invalid_longitude'], ARRAY<STRING>[]
            ),
            IF(source_values.weather_wind_speed < 0, ['invalid_wind_speed'], ARRAY<STRING>[]),
            IF(
                source_values.weather_issue_date IS NOT NULL AND observed_at_utc IS NULL,
                ['invalid_observed_at_utc'], ARRAY<STRING>[]
            )
        ) AS _invalid_issues,
        ARRAY_CONCAT(
            IF(
                source_wind_unit = 'unknown' AND source_values.weather_wind_speed IS NOT NULL,
                ['unverified_wind_unit'], ARRAY<STRING>[]
            ),
            IF(
                source_temperature_unit = 'unknown' AND source_values.weather_temp IS NOT NULL,
                ['unverified_temperature_unit'], ARRAY<STRING>[]
            ),
            IF(
                temp_celsius IS NULL AND humidity_pct IS NULL AND weather_type IS NULL,
                ['no_weather_measurement'], ARRAY<STRING>[]
            )
        ) AS _missing_issues
    FROM normalized AS n
)

SELECT
    * EXCEPT (_invalid_issues, _missing_issues),
    'garmin' AS source_system,
    'activity_weather' AS source_table,
    CAST(activity_id AS STRING) AS source_record_key,
    CASE
        WHEN ARRAY_LENGTH(_invalid_issues) > 0 THEN 'invalid'
        WHEN ARRAY_LENGTH(_missing_issues) > 0 THEN 'partial'
        ELSE 'valid'
    END AS quality_status,
    ARRAY_CONCAT(_invalid_issues, _missing_issues) AS quality_issues
FROM qualified
