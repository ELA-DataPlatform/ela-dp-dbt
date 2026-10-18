{{
    config(
        materialized='incremental',
        unique_key='id',
        merge_update_columns=[
            'popularity',
            'name',
            'images',
            'uri',
            'type',
            'href',
            'genres',
            'followers',
            'external_urls',
            '_ingested_at'
        ],
        tags=['spotify']
    )
}}

-- Reconcile every canonical staging row, including late records and corrections.
SELECT
    id,
    popularity,
    name,
    images,
    uri,
    type,
    href,
    genres,
    followers,
    external_urls,
    _ingested_at
FROM {{ ref('dlk_spotify_legacy_stg__artist_detail') }}
