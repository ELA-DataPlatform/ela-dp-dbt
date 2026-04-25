{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        * EXCEPT (hrvsummary, hrvreadings),

        -- Parse hrvReadings → ARRAY<STRUCT>
        cast(json_value(hrvsummary, '$.weeklyAvg') AS float64) AS hrv_weekly_avg,

        -- Parse hrvSummary (JSON object) → HRV summary fields
        cast(json_value(hrvsummary, '$.lastNightAvg') AS float64) AS hrv_last_night_avg,
        cast(json_value(hrvsummary, '$.lastNight5MinHigh') AS float64) AS hrv_last_night_5min_high,
        cast(json_value(hrvsummary, '$.baseline.lowUpper') AS float64) AS hrv_baseline_low_upper,
        cast(json_value(hrvsummary, '$.baseline.balancedLow') AS float64) AS hrv_baseline_balanced_low,
        cast(json_value(hrvsummary, '$.baseline.balancedUpper') AS float64) AS hrv_baseline_balanced_upper,
        cast(json_value(hrvsummary, '$.baseline.markerValue') AS float64) AS hrv_baseline_marker_value,
        array(
            SELECT
                struct(
                    cast(json_value(item, '$.hrvValue') AS float64) AS hrv_value,
                    json_value(item, '$.readingTimeGMT') AS reading_time_gmt,
                    json_value(item, '$.readingTimeLocal') AS reading_time_local
                )
            FROM unnest(json_query_array(hrvreadings)) AS item
        ) AS hrv_readings,
        json_value(hrvsummary, '$.calendarDate') AS hrv_calendar_date,
        json_value(hrvsummary, '$.status') AS hrv_status,
        json_value(hrvsummary, '$.feedbackPhrase') AS hrv_feedback_phrase,
        json_value(hrvsummary, '$.createTimeStamp') AS hrv_create_timestamp

    FROM {{ source('garmin', 'normalized_hrv') }}
),

deduplicated AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY userprofilepk, date
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
