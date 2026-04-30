{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        * EXCEPT (
            bodybatteryactivityeventlist,
            bodybatterydynamicfeedbackevent,
            endofdaybodybatterydynamicfeedbackevent
        ),

        -- Parse bodyBatteryActivityEventList → ARRAY<STRUCT>
        ARRAY(
            SELECT
                STRUCT(
                    JSON_VALUE(item, '$.eventType') AS event_type,
                    JSON_VALUE(item, '$.eventStartTimeGmt') AS event_start_time_gmt,
                    JSON_VALUE(item, '$.eventUpdateTimeGmt') AS event_update_time_gmt,
                    CAST(JSON_VALUE(item, '$.timezoneOffset') AS INT64) AS timezone_offset,
                    CAST(JSON_VALUE(item, '$.durationInMilliseconds') AS INT64) AS duration_in_milliseconds,
                    CAST(JSON_VALUE(item, '$.bodyBatteryImpact') AS INT64) AS body_battery_impact,
                    JSON_VALUE(item, '$.feedbackType') AS feedback_type,
                    JSON_VALUE(item, '$.shortFeedback') AS short_feedback,
                    CAST(JSON_VALUE(item, '$.deviceId') AS INT64) AS device_id
                )
            FROM UNNEST(JSON_QUERY_ARRAY(bodybatteryactivityeventlist)) AS item
        ) AS body_battery_activity_events,

        -- Parse bodyBatteryDynamicFeedbackEvent → scalar fields
        JSON_VALUE(bodybatterydynamicfeedbackevent, '$.eventTimestampGmt') AS dynamic_feedback_timestamp_gmt,
        JSON_VALUE(bodybatterydynamicfeedbackevent, '$.bodyBatteryLevel') AS dynamic_feedback_battery_level,
        JSON_VALUE(bodybatterydynamicfeedbackevent, '$.feedbackShortType') AS dynamic_feedback_short_type,
        JSON_VALUE(bodybatterydynamicfeedbackevent, '$.feedbackLongType') AS dynamic_feedback_long_type,

        -- Parse endOfDayBodyBatteryDynamicFeedbackEvent → scalar fields
        JSON_VALUE(endofdaybodybatterydynamicfeedbackevent, '$.eventTimestampGmt') AS eod_feedback_timestamp_gmt,
        JSON_VALUE(endofdaybodybatterydynamicfeedbackevent, '$.bodyBatteryLevel') AS eod_feedback_battery_level,
        JSON_VALUE(endofdaybodybatterydynamicfeedbackevent, '$.feedbackShortType') AS eod_feedback_short_type,
        JSON_VALUE(endofdaybodybatterydynamicfeedbackevent, '$.feedbackLongType') AS eod_feedback_long_type

    FROM {{ source('garmin', 'normalized_stats_and_body') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY userprofileid, date
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
