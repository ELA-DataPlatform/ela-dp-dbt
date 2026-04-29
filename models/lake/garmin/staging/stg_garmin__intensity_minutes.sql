{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        * EXCEPT (imvaluesarray, imvaluedescriptorsdtolist),

        -- Parse imValuesArray → ARRAY<STRUCT> (indexed: [timestamp, value])
        array(
            SELECT
                STRUCT(
                    cast(json_value(item, '$[0]') AS int64) AS `timestamp`,
                    cast(json_value(item, '$[1]') AS int64) AS `value`
                )
            FROM unnest(json_query_array(imvaluesarray)) AS item
        ) AS intensity_minutes_values

    FROM {{ source('garmin', 'normalized_intensity_minutes') }}
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
