{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        * EXCEPT (allmetrics),

        -- Parse allMetrics (JSON object) → extract resting heart rate value
        cast(json_value(allmetrics, '$.metricsMap.WELLNESS_RESTING_HEART_RATE[0].value') AS float64)
            AS resting_heart_rate,
        json_value(allmetrics, '$.metricsMap.WELLNESS_RESTING_HEART_RATE[0].calendarDate') AS rhr_calendar_date

    FROM {{ source('garmin', 'normalized_rhr_daily') }}
),

deduplicated AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY userprofileid, date
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
