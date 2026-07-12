SELECT
    track_id
FROM {{ ref('svc_hub__bridge_track_artist') }}
WHERE artist_position = 0
GROUP BY track_id
HAVING COUNT(*) > 1
