{{
    config(
        materialized='incremental',
        unique_key='album_id',
        merge_update_columns=[
            'name',
            'artists',
            'genres',
            'label',
            'popularity',
            'total_tracks',
            'release_date',
            'release_date_precision',
            'album_type',
            'uri',
            'href',
            'external_ids',
            'external_urls',
            'images',
            'tracks',
            'copyrights',
            'available_markets',
            'type',
            '_ingested_at'
        ],
        tags=['spotify']
    )
}}

SELECT
    id AS album_id,
    name,
    artists,
    genres,
    label,
    popularity,
    total_tracks,
    release_date,
    release_date_precision,
    album_type,
    uri,
    href,
    external_ids,
    external_urls,
    images,
    tracks,
    copyrights,
    available_markets,
    type,
    _ingested_at
FROM {{ ref('stg_spotify__album_detail') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}
