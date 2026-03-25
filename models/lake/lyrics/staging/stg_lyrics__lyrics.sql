{{
    config(
        materialized='view',
        tags=['lyrics']
    )
}}

WITH source AS (
    SELECT
        track_id,
        track_name,
        artist_name,
        album_name,
        duration_s,
        found,
        lrclib_id,
        instrumental,
        plain_lyrics,
        fetched_at,
        _ingested_at
    FROM {{ source('lyrics', 'normalized__lyrics') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY track_id
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT(_row_number)
FROM deduplicated
WHERE _row_number = 1
