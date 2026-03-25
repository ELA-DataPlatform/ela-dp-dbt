{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH source AS (
    SELECT
        id,
        collaborative,
        snapshot_id,
        public,
        images,
        owner,
        type,
        primary_color,
        external_urls,
        tracks,
        href,
        uri,
        items,
        name,
        description,
        _ingested_at
    FROM {{ source('spotify_legacy', 'normalized_playlists_legacy') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT(_row_number)
FROM deduplicated
WHERE _row_number = 1
