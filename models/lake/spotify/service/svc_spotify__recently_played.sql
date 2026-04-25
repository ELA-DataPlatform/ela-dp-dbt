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
        partition_by={
            'field': 'played_at',
            'data_type': 'timestamp',
            'granularity': 'month'
        }
    )
}}

SELECT
    played_at,
    track,
    context,
    _ingested_at,
    JSON_VALUE(track, '$.id') AS track_id
FROM {{ ref('stg_spotify__recently_played') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{% endif %}
