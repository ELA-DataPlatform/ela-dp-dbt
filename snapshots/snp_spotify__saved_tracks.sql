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

select
    track_id,
    track.name as track_name,
    track.artists[safe_offset(0)].id as artist_id,
    track.artists[safe_offset(0)].name as artist_name,
    track.album.id as album_id,
    track.album.name as album_name,
    added_at
from {{ ref('svc_spotify__saved_tracks') }}

{% endsnapshot %}
