{{
    config(
        materialized='incremental',
        unique_key='album_id',
        merge_update_columns=[
            'album',
            'added_at',
            '_ingested_at'
        ],
        tags=['spotify']
    )
}}

select
    album.id as album_id,
    album,
    added_at,
    _ingested_at
from {{ ref('stg_spotify__saved_albums') }}
