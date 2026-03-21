{%- set has_legacy = spotify_table_exists('normalized_recently_played_legacy') -%}
{%- set has_new = spotify_table_exists('normalized_recently_played') -%}

{{
    config(
        enabled=(has_legacy or has_new),
        materialized='incremental',
        unique_key=['track_id', 'artist_id'],
        merge_update_columns=['artist_position', '_ingested_at'],
        tags=['spotify']
    )
}}

SELECT *
FROM {{ ref('stg_hub__bridge_track_artist') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{% endif %}
