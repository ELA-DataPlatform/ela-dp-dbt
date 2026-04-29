{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        * EXCEPT (heartratevalues, heartratevaluedescriptors, abnormalhrvaluesarray),

        -- Parse heartRateValues JSON array → ARRAY<STRUCT> (indexed: [timestamp, heartrate])
        array(
            SELECT
                STRUCT(
                    cast(json_value(item, '$[0]') AS INT64) AS `timestamp`,
                    cast(json_value(item, '$[1]') AS INT64) AS `value`
                )
            FROM unnest(json_query_array(heartratevalues)) AS item
        ) AS heart_rate_values,

        -- Parse abnormalHRValuesArray JSON array → ARRAY<STRUCT> (indexed: [timestamp, heartrate])
        array(
            SELECT
                STRUCT(
                    cast(json_value(item, '$[0]') AS INT64) AS `timestamp`,
                    cast(json_value(item, '$[1]') AS INT64) AS `value`
                )
            FROM unnest(json_query_array(abnormalhrvaluesarray)) AS item
        ) AS abnormal_hr_values

    FROM {{ source('garmin', 'normalized_heart_rate') }}
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
