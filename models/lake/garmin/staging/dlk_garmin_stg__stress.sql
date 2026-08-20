{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        * EXCEPT (
            stressvaluesarray, stressvaluedescriptorsdtolist,
            bodybatteryvaluesarray, bodybatteryvaluedescriptorsdtolist
        ),

        -- Parse stressValuesArray → ARRAY<STRUCT> (indexed: [timestamp, stressLevel])
        array(
            SELECT
                STRUCT(
                    cast(json_value(item, '$[0]') AS INT64) AS `timestamp`,
                    cast(json_value(item, '$[1]') AS INT64) AS stress_level
                )
            FROM unnest(json_query_array(stressvaluesarray)) AS item
        ) AS stress_values,

        -- Parse bodyBatteryValuesArray → ARRAY<STRUCT> (indexed: [timestamp, status, level, version])
        array(
            SELECT
                STRUCT(
                    cast(json_value(item, '$[0]') AS INT64) AS `timestamp`,
                    json_value(item, '$[1]') AS body_battery_status,
                    cast(json_value(item, '$[2]') AS INT64) AS body_battery_level,
                    cast(json_value(item, '$[3]') AS FLOAT64) AS body_battery_version
                )
            FROM unnest(json_query_array(bodybatteryvaluesarray)) AS item
        ) AS body_battery_values

    FROM {{ source('garmin', 'normalized_stress') }}
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
