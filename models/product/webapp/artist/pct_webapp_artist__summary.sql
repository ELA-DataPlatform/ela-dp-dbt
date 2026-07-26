{{
    config(
        materialized='table',
        tags=['spotify', 'webapp'],
        cluster_by=['artist_id']
    )
}}

-- One row per listened artist. Feeds the hero card, the all-time rank and the
-- artist selector. Plays are credited to the primary artist of each track
-- (bridge_track_artist.artist_position = 0). The source has no per-stream
-- ms_played, so listening_time_min approximates time with full track duration
-- (SUM(duration_ms) / 60000), consistent with the existing ranking marts.

WITH track_plays AS (
    SELECT
        bta.artist_id,
        fp.track_id,
        fp.played_at,
        COALESCE(t.duration_ms, 0) AS duration_ms
    FROM {{ ref('svc_hub__fact_played') }} AS fp
    INNER JOIN {{ ref('svc_hub__bridge_track_artist') }} AS bta
        ON fp.track_id = bta.track_id AND bta.artist_position = 0
    LEFT JOIN {{ ref('svc_hub__ref_track') }} AS t ON fp.track_id = t.track_id
),

artist_totals AS (
    SELECT
        artist_id,
        MIN(played_at) AS first_listened_at,
        MAX(played_at) AS last_listened_at,
        COUNT(*) AS total_plays,
        {{ milliseconds_to_minutes('SUM(duration_ms)') }} AS total_listening_time_min,
        COUNT(DISTINCT track_id) AS distinct_tracks_listened
    FROM track_plays
    GROUP BY artist_id
),

-- Distinct tracks listened per album_id, used to assess full-album completion.
album_listened AS (
    SELECT
        album_id,
        COUNT(DISTINCT track_id) AS listened_tracks
    FROM {{ ref('svc_hub__fact_played') }}
    WHERE album_id IS NOT NULL
    GROUP BY album_id
),

-- Known studio discography per artist (primary album artist), with completion.
artist_albums AS (
    SELECT
        baa.artist_id,
        al.album_id,
        al.album_type = 'album' AS is_studio_album,
        COALESCE(
            COALESCE(albl.listened_tracks, 0) >= al.total_tracks AND al.total_tracks > 0,
            FALSE
        ) AS is_complete
    FROM {{ ref('svc_hub__bridge_album_artist') }} AS baa
    INNER JOIN {{ ref('svc_hub__ref_album') }} AS al ON baa.album_id = al.album_id
    LEFT JOIN album_listened AS albl ON al.album_id = albl.album_id
    WHERE baa.artist_position = 0
),

artist_studio AS (
    SELECT
        artist_id,
        COUNTIF(is_studio_album) AS studio_albums_total,
        COUNTIF(is_studio_album AND is_complete) AS studio_albums_completed
    FROM artist_albums
    GROUP BY artist_id
),

active_days_numbered AS (
    SELECT
        artist_id,
        listen_date,
        ROW_NUMBER() OVER (
            PARTITION BY artist_id
            ORDER BY listen_date
        ) AS active_day_number
    FROM {{ ref('pct_webapp_artist__listening_daily') }}
    WHERE plays > 0
),

active_day_streaks AS (
    SELECT
        artist_id,
        COUNT(*) AS streak_days
    FROM active_days_numbered
    GROUP BY
        artist_id,
        DATE_SUB(listen_date, INTERVAL active_day_number DAY)
),

max_active_day_streak AS (
    SELECT
        artist_id,
        MAX(streak_days) AS max_active_day_streak_365
    FROM active_day_streaks
    GROUP BY artist_id
),

daily_behavior AS (
    SELECT
        d.artist_id,
        COUNTIF(d.plays > 0) AS active_days_365,
        CAST(ROUND(AVG(IF(d.plays > 0, d.listening_time_min, NULL))) AS INT64)
            AS avg_active_day_listening_time_min,
        MAX(d.listening_time_min) AS max_day_listening_time_min,
        SUM(d.listening_time_min) AS total_listening_time_365d_min
    FROM {{ ref('pct_webapp_artist__listening_daily') }} AS d
    GROUP BY d.artist_id
),

weekly_behavior AS (
    SELECT
        artist_id,
        CAST(ROUND(AVG(listening_time_min)) AS INT64) AS avg_weekly_listening_time_52w_min,
        MAX(listening_time_min) AS max_week_listening_time_min
    FROM {{ ref('pct_webapp_artist__listening_weekly') }}
    GROUP BY artist_id
),

combined AS (
    SELECT
        att.artist_id,
        att.first_listened_at,
        att.last_listened_at,
        att.total_plays,
        att.total_listening_time_min,
        att.distinct_tracks_listened,
        COALESCE(asg.studio_albums_total, 0) AS studio_albums_total,
        COALESCE(asg.studio_albums_completed, 0) AS studio_albums_completed,
        COALESCE(db.active_days_365, 0) AS active_days_365,
        COALESCE(db.avg_active_day_listening_time_min, 0) AS avg_active_day_listening_time_min,
        COALESCE(db.max_day_listening_time_min, 0) AS max_day_listening_time_min,
        COALESCE(db.total_listening_time_365d_min, 0) AS total_listening_time_365d_min,
        COALESCE(ms.max_active_day_streak_365, 0) AS max_active_day_streak_365,
        COALESCE(wb.avg_weekly_listening_time_52w_min, 0) AS avg_weekly_listening_time_52w_min,
        COALESCE(wb.max_week_listening_time_min, 0) AS max_week_listening_time_min
    FROM artist_totals AS att
    LEFT JOIN artist_studio AS asg ON att.artist_id = asg.artist_id
    LEFT JOIN daily_behavior AS db ON att.artist_id = db.artist_id
    LEFT JOIN max_active_day_streak AS ms ON att.artist_id = ms.artist_id
    LEFT JOIN weekly_behavior AS wb ON att.artist_id = wb.artist_id
)

SELECT
    c.artist_id,
    a.artist_name,
    a.artist_image_url,
    c.first_listened_at,
    c.total_listening_time_min,
    c.total_plays,
    c.distinct_tracks_listened,
    c.studio_albums_total,
    c.studio_albums_completed,
    c.active_days_365,
    c.avg_active_day_listening_time_min,
    c.max_day_listening_time_min,
    c.total_listening_time_365d_min,
    c.max_active_day_streak_365,
    c.avg_weekly_listening_time_52w_min,
    c.max_week_listening_time_min,
    c.last_listened_at,
    JSON_VALUE_ARRAY(a.genres) AS genres,
    DATE(c.first_listened_at, 'Europe/Paris') AS first_listened_date,
    EXTRACT(YEAR FROM DATE(c.first_listened_at, 'Europe/Paris')) AS first_listened_year,
    RANK() OVER (
        ORDER BY c.total_listening_time_min DESC, c.total_plays DESC, c.artist_id ASC
    ) AS all_time_rank
FROM combined AS c
LEFT JOIN {{ ref('svc_hub__ref_artist') }} AS a ON c.artist_id = a.artist_id
