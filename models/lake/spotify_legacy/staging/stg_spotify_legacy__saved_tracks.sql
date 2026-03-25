{{
    config(
        materialized='view',
        tags=['spotify'],
        enabled=(target.name != 'prd')
    )
}}

WITH source AS (
    SELECT
        added_at,
        track,
        _ingested_at
    FROM {{ source('spotify_legacy', 'normalized_saved_tracks_legacy') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY track.id
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT(_row_number)
FROM deduplicated
WHERE _row_number = 1
