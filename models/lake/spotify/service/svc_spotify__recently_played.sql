{%- set has_legacy = spotify_table_exists('normalized_recently_played_legacy') -%}
{%- set has_new = spotify_table_exists('normalized_recently_played') -%}

{{
    config(
        materialized='incremental',
        unique_key=['played_at', 'track_id'],
        merge_update_columns=[
            'track',
            'context',
            '_ingested_at'
        ],
        tags=['spotify'],
        enabled=(has_legacy or has_new),
        partition_by={
            'field': 'played_at',
            'data_type': 'timestamp',
            'granularity': 'month'
        }
    )
}}

SELECT
    played_at,
    track.id AS track_id,
    track,
    context,
    _ingested_at
FROM {{ ref('stg_spotify__recently_played') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}
