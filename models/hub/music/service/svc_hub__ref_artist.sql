{{
    config(
        materialized='incremental',
        unique_key='artist_id',
        merge_update_columns=['artist_name', 'artist_uri', 'genres', 'followers_total', 'popularity', '_ingested_at'],
        tags=['spotify']
    )
}}

SELECT *
FROM {{ ref('stg_hub__ref_artist') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{% endif %}
