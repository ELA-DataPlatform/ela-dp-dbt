{{
    config(
        materialized='incremental',
        unique_key='album_id',
        merge_update_columns=[
            'album_name', 'album_type', 'total_tracks',
            'release_date', 'release_date_precision', 'album_uri',
            'genres', 'label', 'popularity', '_ingested_at'
        ],
        tags=['spotify']
    )
}}

SELECT *
FROM {{ ref('stg_hub__ref_album') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{% endif %}
