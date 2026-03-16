{% snapshot snp_spotify__saved_albums %}

{{
    config(
        target_schema='dp_lake_spotify_' ~ target.name,
        unique_key='album_id',
        strategy='check',
        check_cols=['album_name', 'artist_name', 'added_at'],
        invalidate_hard_deletes=True,
        tags=['spotify']
    )
}}

select
    album_id,
    album.name as album_name,
    album.artists[safe_offset(0)].id as artist_id,
    album.artists[safe_offset(0)].name as artist_name,
    added_at
from {{ ref('svc_spotify__saved_albums') }}

{% endsnapshot %}
