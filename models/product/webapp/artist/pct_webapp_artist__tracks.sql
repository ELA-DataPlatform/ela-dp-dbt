{{
    config(
        materialized='table',
        tags=['spotify', 'webapp'],
        cluster_by=['artist_id']
    )
}}

-- One row per (artist_id, track_id) for tracks credited to the primary artist
-- (bridge_track_artist.artist_position = 0). Feeds the per-artist track ranking
-- (order by listening_time_min). All-time scope. listening_time_min approximates
-- time with full track duration; see schema.yml.

WITH track_plays AS (
    SELECT
        bta.artist_id,
        fp.track_id,
        COUNT(*) AS plays,
        {{ milliseconds_to_minutes('SUM(COALESCE(t.duration_ms, 0))') }} AS listening_time_min
    FROM {{ ref('svc_hub__fact_played') }} AS fp
    INNER JOIN {{ ref('svc_hub__bridge_track_artist') }} AS bta
        ON fp.track_id = bta.track_id AND bta.artist_position = 0
    LEFT JOIN {{ ref('svc_hub__ref_track') }} AS t ON fp.track_id = t.track_id
    GROUP BY bta.artist_id, fp.track_id
)

SELECT
    tp.artist_id,
    tp.track_id,
    t.track_name,
    t.album_id,
    al.album_name,
    tp.listening_time_min,
    tp.plays,
    RANK() OVER (
        PARTITION BY tp.artist_id
        ORDER BY tp.listening_time_min DESC, tp.plays DESC, tp.track_id ASC
    ) AS artist_track_rank
FROM track_plays AS tp
LEFT JOIN {{ ref('svc_hub__ref_track') }} AS t ON tp.track_id = t.track_id
LEFT JOIN {{ ref('svc_hub__ref_album') }} AS al ON t.album_id = al.album_id
