{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH source AS (
    SELECT
        id,
        external_ids,
        copyrights,
        artists,
        release_date,
        label,
        uri,
        name,
        popularity,
        type,
        href,
        genres,
        tracks,
        external_urls,
        total_tracks,
        release_date_precision,
        images,
        available_markets,
        album_type,
        _ingested_at
    FROM {{ source('spotify_legacy', 'normalized_album_detail_legacy') }}
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

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
