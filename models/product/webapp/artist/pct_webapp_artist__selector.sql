{{
    config(
        materialized='table',
        tags=['spotify', 'webapp']
    )
}}

-- Artists eligible for the app dropdown: at least 10 minutes of all-time
-- listening. Ordered by total_listening_time_min desc so the most-listened
-- artists appear first in the list.

WITH artist_listening AS (
    SELECT
        bta.artist_id,
        CAST(SUM(COALESCE(t.duration_ms, 0)) / 60000 AS INT64) AS total_listening_time_min
    FROM {{ ref('svc_hub__fact_played') }} AS fp
    INNER JOIN {{ ref('svc_hub__bridge_track_artist') }} AS bta
        ON fp.track_id = bta.track_id AND bta.artist_position = 0
    LEFT JOIN {{ ref('svc_hub__ref_track') }} AS t ON fp.track_id = t.track_id
    GROUP BY bta.artist_id
    HAVING CAST(SUM(COALESCE(t.duration_ms, 0)) / 60000 AS INT64) >= 60
)

SELECT
    al.artist_id,
    a.artist_name,
    al.total_listening_time_min
FROM artist_listening AS al
LEFT JOIN {{ ref('svc_hub__ref_artist') }} AS a ON al.artist_id = a.artist_id
ORDER BY al.total_listening_time_min DESC
