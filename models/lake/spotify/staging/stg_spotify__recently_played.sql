{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('spotify', 'normalized_recently_played') }}
),

deduplicated AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY played_at, track.id
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
