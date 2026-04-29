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
            bodybatterydynamicfeedbackevent, endofdaybodybatterydynamicfeedbackevent,
            stress, bodybattery
        ),

        -- Parse bodyBatteryValuesArray → ARRAY<STRUCT> (old format: [{timestamp, value}])
        ARRAY(
            SELECT
                STRUCT(
                    CAST(JSON_VALUE(item, '$.timestamp') AS INT64) AS `timestamp`,
                    CAST(JSON_VALUE(item, '$.value') AS INT64) AS `value`
                )
            FROM UNNEST(JSON_QUERY_ARRAY(bodybatteryvaluesarray)) AS item
        ) AS body_battery_values,

        -- Parse bodyBatteryActivityEvent → ARRAY<STRUCT>
        ARRAY(
            SELECT
                STRUCT(
                    JSON_VALUE(item, '$.eventType') AS event_type,
                    JSON_VALUE(item, '$.eventStartTimeGmt') AS event_start_time_gmt,
                    CAST(JSON_VALUE(item, '$.timezoneOffset') AS INT64) AS timezone_offset,
                    CAST(JSON_VALUE(item, '$.durationInMilliseconds') AS INT64) AS duration_in_milliseconds,
                    CAST(JSON_VALUE(item, '$.bodyBatteryImpact') AS INT64) AS body_battery_impact,
                    JSON_VALUE(item, '$.feedbackType') AS feedback_type,
                    JSON_VALUE(item, '$.shortFeedback') AS short_feedback
                )
            FROM UNNEST(JSON_QUERY_ARRAY(bodybatteryactivityevent)) AS item
        ) AS body_battery_activity_events,

        -- Parse bodyBatteryDynamicFeedbackEvent (JSON object)
        JSON_VALUE(bodybatterydynamicfeedbackevent, '$.eventTimestampGmt') AS dynamic_feedback_timestamp_gmt,
        JSON_VALUE(bodybatterydynamicfeedbackevent, '$.bodyBatteryLevel') AS dynamic_feedback_battery_level,
        JSON_VALUE(bodybatterydynamicfeedbackevent, '$.feedbackShortType') AS dynamic_feedback_short_type,
        JSON_VALUE(bodybatterydynamicfeedbackevent, '$.feedbackLongType') AS dynamic_feedback_long_type,

        -- Parse endOfDayBodyBatteryDynamicFeedbackEvent (JSON object)
        JSON_VALUE(endofdaybodybatterydynamicfeedbackevent, '$.eventTimestampGmt') AS eod_feedback_timestamp_gmt,
        JSON_VALUE(endofdaybodybatterydynamicfeedbackevent, '$.bodyBatteryLevel') AS eod_feedback_battery_level,
        JSON_VALUE(endofdaybodybatterydynamicfeedbackevent, '$.feedbackShortType') AS eod_feedback_short_type,
        JSON_VALUE(endofdaybodybatterydynamicfeedbackevent, '$.feedbackLongType') AS eod_feedback_long_type,

        -- Parse bodyBattery → ARRAY<STRUCT> (new format: {labels, data: [[ts_gmt, val, status, version]]})
        ARRAY(
            SELECT
                STRUCT(
                    JSON_VALUE(item, '$[0]') AS timestamp_gmt,
                    CAST(JSON_VALUE(item, '$[1]') AS INT64) AS `value`,
                    CAST(JSON_VALUE(item, '$[2]') AS INT64) AS status,
                    CAST(JSON_VALUE(item, '$[3]') AS FLOAT64) AS version
                )
            FROM UNNEST(JSON_QUERY_ARRAY(bodybattery, '$.data')) AS item
        ) AS body_battery_timeseries,

        -- Parse stress → ARRAY<STRUCT> (new format: {labels, data: [[ts_gmt, stress_level]]})
        ARRAY(
            SELECT
                STRUCT(
                    JSON_VALUE(item, '$[0]') AS timestamp_gmt,
                    CAST(JSON_VALUE(item, '$[1]') AS INT64) AS stress_level
                )
            FROM UNNEST(JSON_QUERY_ARRAY(stress, '$.data')) AS item
        ) AS stress_timeseries

    FROM {{ source('garmin', 'normalized_body_battery') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY date
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
