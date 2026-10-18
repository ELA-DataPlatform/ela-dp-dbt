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
    album.id AS album_id,
    album,
    added_at,
    _ingested_at
FROM {{ ref('dlk_spotify_legacy_stg__saved_albums') }}
