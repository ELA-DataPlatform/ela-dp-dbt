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

        -- Parse stressValuesArray → ARRAY<STRUCT>
        array(
            SELECT
                struct(
                    cast(json_value(item, '$.timestamp') AS int64) AS `timestamp`,
                    cast(json_value(item, '$.type') AS int64) AS stress_level
                )
            FROM unnest(json_query_array(stressvaluesarray)) AS item
        ) AS stress_values,

        -- Parse bodyBatteryValuesArray → ARRAY<STRUCT>
        array(
            SELECT
                struct(
                    cast(json_value(item, '$.timestamp') AS int64) AS `timestamp`,
                    json_value(item, '$.type') AS `type`,
                    cast(json_value(item, '$.value') AS int64) AS `value`,
                    cast(json_value(item, '$.score') AS float64) AS score
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
