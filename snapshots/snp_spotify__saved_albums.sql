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

    SELECT
        album_id,
        album.name AS album_name,
        added_at,
        album.artists[safe_offset(0)].id AS artist_id,
        album.artists[safe_offset(0)].name AS artist_name
    FROM {{ ref('svc_spotify__saved_albums') }}

{% endsnapshot %}
