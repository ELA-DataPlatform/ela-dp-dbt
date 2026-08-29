SELECT
    album_id
FROM {{ ref('hub_music_svc__bridge_album_artist') }}
WHERE artist_position = 0
GROUP BY album_id
HAVING COUNT(*) > 1
