{{
    config(
        materialized='incremental',
        unique_key='artist_id',
        merge_update_columns=[
            'name',
            'images',
            'uri',
            'type',
            'href',
            'genres',
            'followers',
            'external_urls',
            'popularity',
            '_ingested_at'
        ],
        tags=['spotify']
    )
}}

-- Reconcile every canonical staging row, including late records and corrections.
SELECT
    id AS artist_id,
    name,
    images,
    uri,
    type,
    href,
    genres,
    followers,
    external_urls,
    popularity,
    _ingested_at
FROM {{ ref('dlk_spotify_stg__top_artists') }}
