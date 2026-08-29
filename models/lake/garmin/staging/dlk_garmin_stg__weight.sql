{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT * FROM {{ source('garmin', 'normalized_weight') }}
),

deduplicated AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY date, timestampgmt
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
