{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH from_artist_detail AS (
    SELECT
        artist_id,
        name AS artist_name,
        uri AS artist_uri,
        genres,
        followers.total AS followers_total,
        popularity,
        images[SAFE_OFFSET(0)].url AS image_url,
        _ingested_at,
        0 AS _source_priority
    FROM {{ ref('svc_spotify__artist_detail') }}
),

from_top_artists AS (
    SELECT
        artist_id,
        name AS artist_name,
        uri AS artist_uri,
        genres,
        followers.total AS followers_total,
        popularity,
        CAST(NULL AS STRING) AS image_url,
        _ingested_at,
        0 AS _source_priority
    FROM {{ ref('svc_spotify__top_artists') }}
),

from_recently_played_track AS (
    SELECT
        artist.id AS artist_id,
        artist.name AS artist_name,
        artist.uri AS artist_uri,
        CAST(NULL AS ARRAY<STRING>) AS genres,
        CAST(NULL AS INT64) AS followers_total,
        CAST(NULL AS INT64) AS popularity,
        CAST(NULL AS STRING) AS image_url,
        _ingested_at,
        1 AS _source_priority
    FROM {{ ref('svc_spotify__recently_played') }},
        UNNEST(track.artists) AS artist
),

from_recently_played_album AS (
    SELECT
        artist.id AS artist_id,
        artist.name AS artist_name,
        artist.uri AS artist_uri,
        CAST(NULL AS ARRAY<STRING>) AS genres,
        CAST(NULL AS INT64) AS followers_total,
        CAST(NULL AS INT64) AS popularity,
        CAST(NULL AS STRING) AS image_url,
        _ingested_at,
        1 AS _source_priority
    FROM {{ ref('svc_spotify__recently_played') }},
        UNNEST(track.album.artists) AS artist
),

combined AS (
    SELECT * FROM from_artist_detail
    UNION ALL
    SELECT * FROM from_top_artists
    UNION ALL
    SELECT * FROM from_recently_played_track
    UNION ALL
    SELECT * FROM from_recently_played_album
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY artist_id
            ORDER BY _source_priority ASC, _ingested_at DESC
        ) AS _row_number
    FROM combined
)

SELECT * EXCEPT (_row_number, _source_priority)
FROM deduplicated
WHERE _row_number = 1
