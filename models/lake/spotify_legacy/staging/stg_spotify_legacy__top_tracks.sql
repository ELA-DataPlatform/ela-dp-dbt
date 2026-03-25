{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH source AS (
    SELECT
        id,
        type,
        popularity,
        uri,
        name,
        is_playable,
        href,
        external_urls,
        preview_url,
        duration_ms,
        explicit,
        track_number,
        external_ids,
        is_local,
        disc_number,
        available_markets,
        artists,
        album,
        _ingested_at
    FROM {{ source('spotify_legacy', 'normalized_top_tracks_legacy') }}
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
