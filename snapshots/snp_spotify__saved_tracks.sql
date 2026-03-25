{% snapshot snp_spotify__saved_tracks %}

{{
    config(
        target_schema='dp_lake_spotify_' ~ target.name,
        unique_key='track_id',
        strategy='check',
        check_cols=['track_name', 'artist_name', 'album_name', 'added_at'],
        invalidate_hard_deletes=True,
        tags=['spotify']
    )
}}

    SELECT
        track_id,
        track.name AS track_name,
        track.album.id AS album_id,
        track.album.name AS album_name,
        added_at,
        track.artists[safe_offset(0)].id AS artist_id,
        track.artists[safe_offset(0)].name AS artist_name
    FROM {{ ref('svc_spotify__saved_tracks') }}

{% endsnapshot %}
