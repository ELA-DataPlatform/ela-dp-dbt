{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        * EXCEPT (
            allmetrics,
            restingheartrate,
            heartratevalues, heartratevaluedescriptors,
            abnormalhrvaluesarray
        ),

        -- Resting HR: new format → flat restingHeartRate / old format → allMetrics JSON
        CAST(COALESCE(
            CAST(restingheartrate AS FLOAT64),
            CAST(JSON_VALUE(allmetrics, '$.metricsMap.WELLNESS_RESTING_HEART_RATE[0].value') AS FLOAT64)
        ) AS INT64) AS resting_heart_rate,

        -- Parse heartRateValues → ARRAY<STRUCT> (format: [[timestamp_ms, hr_value], ...])
        ARRAY(
            SELECT
                STRUCT(
                    CAST(JSON_VALUE(item, '$[0]') AS INT64) AS `timestamp`,
                    CAST(JSON_VALUE(item, '$[1]') AS INT64) AS heart_rate
                )
            FROM UNNEST(JSON_QUERY_ARRAY(heartratevalues)) AS item
        ) AS heart_rate_values,

        -- Parse abnormalHRValuesArray → ARRAY<STRUCT> (format: [[timestamp_ms, hr_value], ...])
        ARRAY(
            SELECT
                STRUCT(
                    CAST(JSON_VALUE(item, '$[0]') AS INT64) AS `timestamp`,
                    CAST(JSON_VALUE(item, '$[1]') AS INT64) AS heart_rate
                )
            FROM UNNEST(JSON_QUERY_ARRAY(abnormalhrvaluesarray)) AS item
        ) AS abnormal_hr_values

    FROM {{ source('garmin', 'normalized_rhr_daily') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(userprofileid, userprofilepk), date
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
