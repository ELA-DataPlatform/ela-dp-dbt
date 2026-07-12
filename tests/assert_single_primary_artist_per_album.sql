SELECT
    album_id
FROM {{ ref('svc_hub__bridge_album_artist') }}
WHERE artist_position = 0
GROUP BY album_id
HAVING COUNT(*) > 1
