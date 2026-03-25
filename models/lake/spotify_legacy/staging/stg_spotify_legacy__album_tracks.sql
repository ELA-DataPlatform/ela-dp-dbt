{{
    config(
        materialized='view',
        tags=['spotify'],
        enabled=(target.name != 'prd')
    )
}}

WITH source AS (
    SELECT
        album_id,
        id,
        restrictions,
        type,
        duration_ms,
        explicit,
        preview_url,
        uri,
        name,
        href,
        external_urls,
        is_local,
        disc_number,
        available_markets,
        track_number,
        artists,
        _ingested_at
    FROM {{ source('spotify_legacy', 'normalized_album_tracks_legacy') }}
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
