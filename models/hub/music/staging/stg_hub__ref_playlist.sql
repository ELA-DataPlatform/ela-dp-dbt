{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

SELECT
    playlist_id,
    name AS playlist_name,
    description,
    collaborative,
    public,
    snapshot_id,
    owner.id AS owner_id,
    tracks.total AS total_tracks,
    uri AS playlist_uri,
    _ingested_at
FROM {{ ref('svc_spotify__playlists') }}
