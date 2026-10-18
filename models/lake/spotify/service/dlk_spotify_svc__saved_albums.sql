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

-- Reconcile every canonical staging row, including late records and corrections.
SELECT
    album,
    added_at,
    _ingested_at,
    JSON_VALUE(album, '$.id') AS album_id
FROM {{ ref('dlk_spotify_stg__saved_albums') }}
