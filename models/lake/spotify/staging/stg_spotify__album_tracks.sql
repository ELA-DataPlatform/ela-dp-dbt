{%- set has_legacy = spotify_table_exists('normalized_album_tracks_legacy') -%}

{{
    config(
        materialized='view',
        tags=['spotify'],
        enabled=has_legacy
    )
}}

WITH source AS (
    SELECT * FROM {{ source('spotify', 'normalized_album_tracks_legacy') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY album_id, id
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT(_row_number)
FROM deduplicated
WHERE _row_number = 1
