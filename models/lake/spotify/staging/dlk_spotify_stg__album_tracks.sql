{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('spotify', 'normalized_album_tracks') }}
),

deduplicated AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY album_id, id
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
