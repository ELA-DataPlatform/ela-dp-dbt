{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        * EXCEPT (
            bodybatteryvaluesarray, bodybatteryvaluedescriptordtolist,
            bodybatteryactivityevent,
            bodybatterydynamicfeedbackevent, endofdaybodybatterydynamicfeedbackevent
        ),

        -- Parse bodyBatteryValuesArray → ARRAY<STRUCT>
        array(
            SELECT
                struct(
                    cast(json_value(item, '$.timestamp') AS int64) AS `timestamp`,
                    cast(json_value(item, '$.value') AS int64) AS `value`
                )
            FROM unnest(json_query_array(bodybatteryvaluesarray)) AS item
        ) AS body_battery_values,

        -- Parse bodyBatteryActivityEvent → ARRAY<STRUCT>
        array(
            SELECT
                struct(
                    json_value(item, '$.eventType') AS event_type,
                    json_value(item, '$.eventStartTimeGmt') AS event_start_time_gmt,
                    cast(json_value(item, '$.timezoneOffset') AS int64) AS timezone_offset,
                    cast(json_value(item, '$.durationInMilliseconds') AS int64) AS duration_in_milliseconds,
                    cast(json_value(item, '$.bodyBatteryImpact') AS int64) AS body_battery_impact,
                    json_value(item, '$.feedbackType') AS feedback_type,
                    json_value(item, '$.shortFeedback') AS short_feedback
                )
            FROM unnest(json_query_array(bodybatteryactivityevent)) AS item
        ) AS body_battery_activity_events,

        -- Parse bodyBatteryDynamicFeedbackEvent (JSON object)
        json_value(bodybatterydynamicfeedbackevent, '$.eventTimestampGmt') AS dynamic_feedback_timestamp_gmt,
        json_value(bodybatterydynamicfeedbackevent, '$.bodyBatteryLevel') AS dynamic_feedback_battery_level,
        json_value(bodybatterydynamicfeedbackevent, '$.feedbackShortType') AS dynamic_feedback_short_type,
        json_value(bodybatterydynamicfeedbackevent, '$.feedbackLongType') AS dynamic_feedback_long_type,

        -- Parse endOfDayBodyBatteryDynamicFeedbackEvent (JSON object)
        json_value(endofdaybodybatterydynamicfeedbackevent, '$.eventTimestampGmt') AS eod_feedback_timestamp_gmt,
        json_value(endofdaybodybatterydynamicfeedbackevent, '$.bodyBatteryLevel') AS eod_feedback_battery_level,
        json_value(endofdaybodybatterydynamicfeedbackevent, '$.feedbackShortType') AS eod_feedback_short_type,
        json_value(endofdaybodybatterydynamicfeedbackevent, '$.feedbackLongType') AS eod_feedback_long_type

    FROM {{ source('garmin', 'normalized_body_battery') }}
),

deduplicated AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY date
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
