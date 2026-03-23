
{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

with source as (
    select
        * except(generic, heatAltitudeAcclimation),

        -- Parse generic (JSON object) → VO2 max fields
        json_value(generic, '$.calendarDate') as vo2_max_calendar_date,
        cast(json_value(generic, '$.vo2MaxPreciseValue') as float64) as vo2_max_precise,
        cast(json_value(generic, '$.vo2MaxValue') as float64) as vo2_max,
        cast(json_value(generic, '$.maxMetCategory') as int64) as max_met_category,

        -- Parse heatAltitudeAcclimation (JSON object) → acclimation fields
        json_value(heatAltitudeAcclimation, '$.calendarDate') as acclimation_calendar_date,
        json_value(heatAltitudeAcclimation, '$.altitudeAcclimationDate') as altitude_acclimation_date,
        json_value(heatAltitudeAcclimation, '$.heatAcclimationDate') as heat_acclimation_date,
        cast(json_value(heatAltitudeAcclimation, '$.altitudeAcclimation') as float64) as altitude_acclimation,
        cast(json_value(heatAltitudeAcclimation, '$.previousAltitudeAcclimation') as float64) as previous_altitude_acclimation,
        cast(json_value(heatAltitudeAcclimation, '$.heatAcclimationPercentage') as float64) as heat_acclimation_pct,
        cast(json_value(heatAltitudeAcclimation, '$.previousHeatAcclimationPercentage') as float64) as previous_heat_acclimation_pct,
        cast(json_value(heatAltitudeAcclimation, '$.currentAltitude') as float64) as current_altitude,
        cast(json_value(heatAltitudeAcclimation, '$.acclimationPercentage') as float64) as acclimation_pct

    from {{ source('garmin', 'normalized_max_metrics') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by userId, date
            order by _ingested_at desc
        ) as _row_number
    from source
)

select * except(_row_number)
from deduplicated
where _row_number = 1
