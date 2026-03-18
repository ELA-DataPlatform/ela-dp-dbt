{{
    config(
        materialized='incremental',
        unique_key=['album_id', 'artist_id'],
        merge_update_columns=['artist_position', '_ingested_at'],
        tags=['spotify']
    )
}}

SELECT *
FROM {{ ref('stg_hub__bridge_album_artist') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{% endif %}
