{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        * EXCEPT (weather_data),

        -- Parse weather_data (JSON object) → weather fields
        cast(json_value(weather_data, '$.temp') AS float64) AS weather_temp,
        cast(json_value(weather_data, '$.apparentTemp') AS float64) AS weather_apparent_temp,
        cast(json_value(weather_data, '$.dewPoint') AS float64) AS weather_dew_point,
        cast(json_value(weather_data, '$.relativeHumidity') AS float64) AS weather_relative_humidity,
        cast(json_value(weather_data, '$.windDirection') AS float64) AS weather_wind_direction,
        cast(json_value(weather_data, '$.windSpeed') AS float64) AS weather_wind_speed,
        cast(json_value(weather_data, '$.latitude') AS float64) AS weather_latitude,
        cast(json_value(weather_data, '$.longitude') AS float64) AS weather_longitude,
        json_value(weather_data, '$.issueDate') AS weather_issue_date,
        json_value(weather_data, '$.windDirectionCompassPoint') AS weather_wind_direction_compass,
        json_value(weather_data, '$.weatherStationDTO.id') AS weather_station_id,
        json_value(weather_data, '$.weatherStationDTO.name') AS weather_station_name,
        json_value(weather_data, '$.weatherTypeDTO.desc') AS weather_type

    FROM {{ source('garmin', 'normalized_activity_weather') }}
),

deduplicated AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY activityid
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
