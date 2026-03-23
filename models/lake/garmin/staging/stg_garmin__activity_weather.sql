
{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

with source as (
    select
        * except(weather_data),

        -- Parse weather_data (JSON object) → weather fields
        json_value(weather_data, '$.issueDate') as weather_issue_date,
        cast(json_value(weather_data, '$.temp') as float64) as weather_temp,
        cast(json_value(weather_data, '$.apparentTemp') as float64) as weather_apparent_temp,
        cast(json_value(weather_data, '$.dewPoint') as float64) as weather_dew_point,
        cast(json_value(weather_data, '$.relativeHumidity') as float64) as weather_relative_humidity,
        cast(json_value(weather_data, '$.windDirection') as float64) as weather_wind_direction,
        json_value(weather_data, '$.windDirectionCompassPoint') as weather_wind_direction_compass,
        cast(json_value(weather_data, '$.windSpeed') as float64) as weather_wind_speed,
        cast(json_value(weather_data, '$.latitude') as float64) as weather_latitude,
        cast(json_value(weather_data, '$.longitude') as float64) as weather_longitude,
        json_value(weather_data, '$.weatherStationDTO.id') as weather_station_id,
        json_value(weather_data, '$.weatherStationDTO.name') as weather_station_name,
        json_value(weather_data, '$.weatherTypeDTO.desc') as weather_type

    from {{ source('garmin', 'normalized_activity_weather') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by activityId
            order by _ingested_at desc
        ) as _row_number
    from source
)

select * except(_row_number)
from deduplicated
where _row_number = 1
