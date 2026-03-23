
{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

with source as (
    select
        * except(bodyBatteryValuesArray, bodyBatteryValueDescriptorDTOList,
                 bodyBatteryActivityEvent,
                 bodyBatteryDynamicFeedbackEvent, endOfDayBodyBatteryDynamicFeedbackEvent),

        -- Parse bodyBatteryValuesArray → ARRAY<STRUCT>
        array(
            select struct(
                cast(json_value(item, '$.timestamp') as int64) as timestamp,
                cast(json_value(item, '$.value') as int64) as value
            )
            from unnest(json_query_array(bodyBatteryValuesArray)) as item
        ) as body_battery_values,

        -- Parse bodyBatteryActivityEvent → ARRAY<STRUCT>
        array(
            select struct(
                json_value(item, '$.eventType') as event_type,
                json_value(item, '$.eventStartTimeGmt') as event_start_time_gmt,
                cast(json_value(item, '$.timezoneOffset') as int64) as timezone_offset,
                cast(json_value(item, '$.durationInMilliseconds') as int64) as duration_in_milliseconds,
                cast(json_value(item, '$.bodyBatteryImpact') as int64) as body_battery_impact,
                json_value(item, '$.feedbackType') as feedback_type,
                json_value(item, '$.shortFeedback') as short_feedback
            )
            from unnest(json_query_array(bodyBatteryActivityEvent)) as item
        ) as body_battery_activity_events,

        -- Parse bodyBatteryDynamicFeedbackEvent (JSON object)
        json_value(bodyBatteryDynamicFeedbackEvent, '$.eventTimestampGmt') as dynamic_feedback_timestamp_gmt,
        json_value(bodyBatteryDynamicFeedbackEvent, '$.bodyBatteryLevel') as dynamic_feedback_battery_level,
        json_value(bodyBatteryDynamicFeedbackEvent, '$.feedbackShortType') as dynamic_feedback_short_type,
        json_value(bodyBatteryDynamicFeedbackEvent, '$.feedbackLongType') as dynamic_feedback_long_type,

        -- Parse endOfDayBodyBatteryDynamicFeedbackEvent (JSON object)
        json_value(endOfDayBodyBatteryDynamicFeedbackEvent, '$.eventTimestampGmt') as eod_feedback_timestamp_gmt,
        json_value(endOfDayBodyBatteryDynamicFeedbackEvent, '$.bodyBatteryLevel') as eod_feedback_battery_level,
        json_value(endOfDayBodyBatteryDynamicFeedbackEvent, '$.feedbackShortType') as eod_feedback_short_type,
        json_value(endOfDayBodyBatteryDynamicFeedbackEvent, '$.feedbackLongType') as eod_feedback_long_type

    from {{ source('garmin', 'normalized_body_battery') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by date
            order by _ingested_at desc
        ) as _row_number
    from source
)

select * except(_row_number)
from deduplicated
where _row_number = 1
