{{
    config(
        alias='listening_history',
        tags=['spotify'],
        partition_by={'field': 'played_date', 'data_type': 'date', 'granularity': 'month'},
        cluster_by=['track_id', 'primary_artist_id']
    )
}}

-- Aggregate the artist bridge first: a collaboration must remain one listening event.
WITH track_artists AS (
    SELECT
        b.track_id,
        ARRAY_AGG(
            STRUCT(b.artist_id, a.artist_name, b.artist_position)
            ORDER BY b.artist_position, b.artist_id
        ) AS artists,
        MAX(IF(b.artist_position = 0, b.artist_id, NULL)) AS primary_artist_id,
        MAX(IF(b.artist_position = 0, a.artist_name, NULL)) AS primary_artist_name,
        COUNTIF(a.artist_name IS NULL) AS missing_artist_names,
        MAX(b._ingested_at) AS bridge_ingested_at,
        MAX(a._ingested_at) AS artist_ingested_at
    FROM {{ ref('hub_music_svc__bridge_track_artist') }} AS b
    LEFT JOIN {{ ref('hub_music_svc__ref_artist') }} AS a ON b.artist_id = a.artist_id
    GROUP BY b.track_id
)

SELECT  -- noqa: ST06
    TO_HEX(SHA256(TO_JSON_STRING(STRUCT(p.played_at, p.track_id)))) AS listening_id,
    p.played_at,
    DATE(p.played_at, 'Europe/Paris') AS played_date,
    DATETIME(p.played_at, 'Europe/Paris') AS played_at_local,
    p.track_id,
    t.track_name,
    t.track_uri,
    t.duration_ms AS track_duration_ms,
    t.explicit,
    COALESCE(p.album_id, t.album_id) AS album_id,
    al.album_name,
    ar.primary_artist_id,
    ar.primary_artist_name,
    COALESCE(ar.artists, []) AS artists,
    p.context_type,
    p.context_uri,
    IF(
        t.track_name IS NULL OR al.album_name IS NULL
        OR ar.track_id IS NULL OR ar.missing_artist_names > 0,
        'partial', 'valid'
    ) AS quality_status,
    ARRAY_CONCAT(
        IF(t.track_name IS NULL, ['missing_track_metadata'], ARRAY<STRING>[]),
        IF(al.album_name IS NULL, ['missing_album_metadata'], ARRAY<STRING>[]),
        IF(ar.track_id IS NULL, ['missing_artist_relationships'], ARRAY<STRING>[]),
        IF(ar.missing_artist_names > 0, ['missing_artist_names'], ARRAY<STRING>[])
    ) AS quality_issues,
    p._ingested_at,
    (
        SELECT MAX(observed_at)
        FROM UNNEST([
            t._ingested_at, al._ingested_at, ar.bridge_ingested_at, ar.artist_ingested_at
        ]) AS observed_at
    ) AS _metadata_ingested_at,
    'spotify' AS source_system,
    'Europe/Paris' AS timezone,
    CURRENT_TIMESTAMP() AS _product_built_at
FROM {{ ref('hub_music_svc__fact_played') }} AS p
LEFT JOIN {{ ref('hub_music_svc__ref_track') }} AS t ON p.track_id = t.track_id
LEFT JOIN {{ ref('hub_music_svc__ref_album') }} AS al ON COALESCE(p.album_id, t.album_id) = al.album_id
LEFT JOIN track_artists AS ar ON p.track_id = ar.track_id
