{{
    config(
        materialized='view',
        tags=['garmin']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('garmin', 'normalized_endurance_score') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY startDate,
            endDate
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
