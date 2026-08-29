{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        * EXCEPT (
            respirationvaluesarray, respirationaveragesvaluesarray,
            respirationvaluedescriptorsdtolist, respirationaveragesvaluedescriptordtolist
        ),

        -- Parse respirationValuesArray → ARRAY<STRUCT> (indexed: [timestamp, respiration])
        array(
            SELECT
                STRUCT(
                    cast(json_value(item, '$[0]') AS INT64) AS `timestamp`,
                    cast(json_value(item, '$[1]') AS FLOAT64) AS `value`
                )
            FROM unnest(json_query_array(respirationvaluesarray)) AS item
        ) AS respiration_values,

        -- Parse respirationAveragesValuesArray → ARRAY<STRUCT> (indexed: [timestamp, avg, high, low])
        array(
            SELECT
                STRUCT(
                    cast(json_value(item, '$[0]') AS INT64) AS `timestamp`,
                    cast(json_value(item, '$[1]') AS FLOAT64) AS average,
                    cast(json_value(item, '$[2]') AS FLOAT64) AS high,
                    cast(json_value(item, '$[3]') AS FLOAT64) AS low
                )
            FROM unnest(json_query_array(respirationaveragesvaluesarray)) AS item
        ) AS respiration_averages

    FROM {{ source('garmin', 'normalized_respiration') }}
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
