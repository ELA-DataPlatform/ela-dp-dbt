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

SELECT
    album.id AS album_id,
    album,
    added_at,
    _ingested_at
FROM {{ ref('stg_spotify__saved_albums') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}
