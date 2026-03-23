
{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

with source as (
    select
        * except(hrvSummary, hrvReadings),

        -- Parse hrvReadings → ARRAY<STRUCT>
        array(
            select struct(
                cast(json_value(item, '$.hrvValue') as float64) as hrv_value,
                json_value(item, '$.readingTimeGMT') as reading_time_gmt,
                json_value(item, '$.readingTimeLocal') as reading_time_local
            )
            from unnest(json_query_array(hrvReadings)) as item
        ) as hrv_readings,

        -- Parse hrvSummary (JSON object) → HRV summary fields
        json_value(hrvSummary, '$.calendarDate') as hrv_calendar_date,
        cast(json_value(hrvSummary, '$.weeklyAvg') as float64) as hrv_weekly_avg,
        cast(json_value(hrvSummary, '$.lastNightAvg') as float64) as hrv_last_night_avg,
        cast(json_value(hrvSummary, '$.lastNight5MinHigh') as float64) as hrv_last_night_5min_high,
        cast(json_value(hrvSummary, '$.baseline.lowUpper') as float64) as hrv_baseline_low_upper,
        cast(json_value(hrvSummary, '$.baseline.balancedLow') as float64) as hrv_baseline_balanced_low,
        cast(json_value(hrvSummary, '$.baseline.balancedUpper') as float64) as hrv_baseline_balanced_upper,
        cast(json_value(hrvSummary, '$.baseline.markerValue') as float64) as hrv_baseline_marker_value,
        json_value(hrvSummary, '$.status') as hrv_status,
        json_value(hrvSummary, '$.feedbackPhrase') as hrv_feedback_phrase,
        json_value(hrvSummary, '$.createTimeStamp') as hrv_create_timestamp

    from {{ source('garmin', 'normalized_hrv') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by userProfilePk, date
            order by _ingested_at desc
        ) as _row_number
    from source
)

select * except(_row_number)
from deduplicated
where _row_number = 1
