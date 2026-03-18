{{
    config(
        materialized='incremental',
        unique_key=['played_at', 'track_id'],
        merge_update_columns=['album_id', 'context_type', 'context_uri', '_ingested_at'],
        tags=['spotify'],
        partition_by={
            'field': 'played_at',
            'data_type': 'timestamp',
            'granularity': 'month'
        }
    )
}}

SELECT *
FROM {{ ref('stg_hub__fact_played') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{% endif %}
