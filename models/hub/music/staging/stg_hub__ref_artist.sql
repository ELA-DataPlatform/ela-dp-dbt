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
        CAST(JSON_VALUE(followers, '$.total') AS INT64) AS followers_total,
        popularity,
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
        CAST(JSON_VALUE(followers, '$.total') AS INT64) AS followers_total,
        popularity,
        _ingested_at,
        0 AS _source_priority
    FROM {{ ref('svc_spotify__top_artists') }}
),

from_recently_played_track AS (
    SELECT
        JSON_VALUE(artist, '$.id') AS artist_id,
        JSON_VALUE(artist, '$.name') AS artist_name,
        JSON_VALUE(artist, '$.uri') AS artist_uri,
        CAST(NULL AS STRING) AS genres,
        CAST(NULL AS INT64) AS followers_total,
        CAST(NULL AS INT64) AS popularity,
        _ingested_at,
        1 AS _source_priority
    FROM {{ ref('svc_spotify__recently_played') }},
        UNNEST(JSON_QUERY_ARRAY(track, '$.artists')) AS artist
),

from_recently_played_album AS (
    SELECT
        JSON_VALUE(artist, '$.id') AS artist_id,
        JSON_VALUE(artist, '$.name') AS artist_name,
        JSON_VALUE(artist, '$.uri') AS artist_uri,
        CAST(NULL AS STRING) AS genres,
        CAST(NULL AS INT64) AS followers_total,
        CAST(NULL AS INT64) AS popularity,
        _ingested_at,
        1 AS _source_priority
    FROM {{ ref('svc_spotify__recently_played') }},
        UNNEST(JSON_QUERY_ARRAY(track, '$.album.artists')) AS artist
),

from_legacy_artist_detail AS (
    SELECT
        id AS artist_id,
        name AS artist_name,
        uri AS artist_uri,
        TO_JSON_STRING(genres) AS genres,
        followers.total AS followers_total,
        popularity,
        _ingested_at,
        0 AS _source_priority
    FROM {{ ref('svc_spotify_legacy__artist_detail') }}
),

from_legacy_top_artists AS (
    SELECT
        id AS artist_id,
        name AS artist_name,
        uri AS artist_uri,
        TO_JSON_STRING(genres) AS genres,
        followers.total AS followers_total,
        popularity,
        _ingested_at,
        0 AS _source_priority
    FROM {{ ref('svc_spotify_legacy__top_artists') }}
),

from_legacy_recently_played_track AS (
    SELECT
        artist.id AS artist_id,
        artist.name AS artist_name,
        artist.uri AS artist_uri,
        CAST(NULL AS STRING) AS genres,
        CAST(NULL AS INT64) AS followers_total,
        CAST(NULL AS INT64) AS popularity,
        _ingested_at,
        1 AS _source_priority
    FROM {{ ref('svc_spotify_legacy__recently_played') }},
        UNNEST(track.artists) AS artist
),

from_legacy_recently_played_album AS (
    SELECT
        artist.id AS artist_id,
        artist.name AS artist_name,
        artist.uri AS artist_uri,
        CAST(NULL AS STRING) AS genres,
        CAST(NULL AS INT64) AS followers_total,
        CAST(NULL AS INT64) AS popularity,
        _ingested_at,
        1 AS _source_priority
    FROM {{ ref('svc_spotify_legacy__recently_played') }},
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
    UNION ALL
    SELECT * FROM from_legacy_artist_detail
    UNION ALL
    SELECT * FROM from_legacy_top_artists
    UNION ALL
    SELECT * FROM from_legacy_recently_played_track
    UNION ALL
    SELECT * FROM from_legacy_recently_played_album
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
