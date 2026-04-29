{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        * EXCEPT (floorvaluesarray, floorsvaluedescriptordtolist),

        -- Parse floorValuesArray → ARRAY<STRUCT> (indexed: [startTimeGMT, endTimeGMT, floorsAscended, floorsDescended])
        array(
            SELECT
                STRUCT(
                    json_value(item, '$[0]') AS start_time,
                    json_value(item, '$[1]') AS end_time,
                    cast(json_value(item, '$[2]') AS int64) AS ascended,
                    cast(json_value(item, '$[3]') AS int64) AS descended
                )
            FROM unnest(json_query_array(floorvaluesarray)) AS item
        ) AS floor_values

    FROM {{ source('garmin', 'normalized_floors') }}
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
