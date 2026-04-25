{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH from_album_detail AS (
    SELECT
        album_id,
        name AS album_name,
        album_type,
        total_tracks,
        release_date,
        release_date_precision,
        uri AS album_uri,
        genres,
        label,
        popularity,
        _ingested_at,
        0 AS _source_priority
    FROM {{ ref('svc_spotify__album_detail') }}
),

from_recently_played AS (
    SELECT
        track.album.id AS album_id,
        track.album.name AS album_name,
        track.album.album_type,
        track.album.total_tracks,
        CAST(track.album.release_date AS STRING) AS release_date,
        track.album.release_date_precision,
        track.album.uri AS album_uri,
        CAST(NULL AS ARRAY<STRING>) AS genres,
        CAST(NULL AS STRING) AS label,
        CAST(NULL AS INT64) AS popularity,
        _ingested_at,
        1 AS _source_priority
    FROM {{ ref('svc_spotify__recently_played') }}
),

combined AS (
    SELECT * FROM from_album_detail
    UNION ALL
    SELECT * FROM from_recently_played
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY album_id
            ORDER BY _source_priority ASC, _ingested_at DESC
        ) AS _row_number
    FROM combined
)

SELECT * EXCEPT (_row_number, _source_priority)
FROM deduplicated
WHERE _row_number = 1
