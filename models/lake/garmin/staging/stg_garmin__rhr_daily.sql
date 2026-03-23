
{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

with source as (
    select
        * except(allMetrics),

        -- Parse allMetrics (JSON object) → extract resting heart rate value
        cast(json_value(allMetrics, '$.metricsMap.WELLNESS_RESTING_HEART_RATE[0].value') as float64) as resting_heart_rate,
        json_value(allMetrics, '$.metricsMap.WELLNESS_RESTING_HEART_RATE[0].calendarDate') as rhr_calendar_date

    from {{ source('garmin', 'normalized_rhr_daily') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by userProfileId, date
            order by _ingested_at desc
        ) as _row_number
    from source
)

select * except(_row_number)
from deduplicated
where _row_number = 1
