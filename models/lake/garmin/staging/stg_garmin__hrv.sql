{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH old_format AS (
    -- Original API: one row per date, flat hrvSummary object + hrvReadings array
    SELECT
        _ingested_at,
        data_type,
        userprofilepk,
        date,
        cast(json_value(hrvsummary, '$.calendarDate') AS date) AS hrv_calendar_date,
        json_value(hrvsummary, '$.status') AS hrv_status,
        json_value(hrvsummary, '$.feedbackPhrase') AS hrv_feedback_phrase,
        cast(json_value(hrvsummary, '$.lastNightAvg') AS float64) AS hrv_last_night_avg,
        cast(json_value(hrvsummary, '$.lastNight5MinHigh') AS float64) AS hrv_last_night_5min_high,
        cast(json_value(hrvsummary, '$.weeklyAvg') AS float64) AS hrv_weekly_avg,
        cast(json_value(hrvsummary, '$.baseline.lowUpper') AS float64) AS hrv_baseline_low_upper,
        cast(json_value(hrvsummary, '$.baseline.balancedLow') AS float64) AS hrv_baseline_balanced_low,
        cast(json_value(hrvsummary, '$.baseline.balancedUpper') AS float64) AS hrv_baseline_balanced_upper,
        cast(json_value(hrvsummary, '$.baseline.markerValue') AS float64) AS hrv_baseline_marker_value,
        array(
            SELECT AS STRUCT
                cast(json_value(item, '$.hrvValue') AS float64) AS hrv_value,
                json_value(item, '$.readingTimeGMT') AS reading_time_gmt,
                json_value(item, '$.readingTimeLocal') AS reading_time_local
            FROM unnest(json_query_array(hrvreadings)) AS item
        ) AS hrv_readings
    FROM {{ source('garmin', 'normalized_hrv') }}
    WHERE date IS NOT NULL AND hrvsummary IS NOT NULL
),

new_format AS (
    -- New API (from ~2026-03): one row contains hrvSummaries JSON array
    -- with multiple calendarDate entries — unnest to one row per date
    SELECT
        n._ingested_at,
        n.data_type,
        n.userprofilepk,
        cast(json_value(item, '$.calendarDate') AS date) AS date,
        cast(json_value(item, '$.calendarDate') AS date) AS hrv_calendar_date,
        json_value(item, '$.status') AS hrv_status,
        json_value(item, '$.feedbackPhrase') AS hrv_feedback_phrase,
        cast(json_value(item, '$.lastNightAvg') AS float64) AS hrv_last_night_avg,
        cast(json_value(item, '$.lastNight5MinHigh') AS float64) AS hrv_last_night_5min_high,
        cast(json_value(item, '$.weeklyAvg') AS float64) AS hrv_weekly_avg,
        cast(json_value(item, '$.baseline.lowUpper') AS float64) AS hrv_baseline_low_upper,
        cast(json_value(item, '$.baseline.balancedLow') AS float64) AS hrv_baseline_balanced_low,
        cast(json_value(item, '$.baseline.balancedUpper') AS float64) AS hrv_baseline_balanced_upper,
        cast(json_value(item, '$.baseline.markerValue') AS float64) AS hrv_baseline_marker_value,
        CAST([] AS ARRAY<STRUCT<hrv_value FLOAT64, reading_time_gmt STRING, reading_time_local STRING>>) AS hrv_readings
    FROM {{ source('garmin', 'normalized_hrv') }} AS n
    CROSS JOIN UNNEST(json_query_array(n.hrvsummaries)) AS item
    WHERE n.date IS NULL AND n.hrvsummaries IS NOT NULL
),

combined AS (
    SELECT * FROM old_format
    UNION ALL
    SELECT * FROM new_format
),

deduplicated AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY userprofilepk, date
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM combined
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
