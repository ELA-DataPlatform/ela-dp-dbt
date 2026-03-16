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

select
    played_at,
    track.id as track_id,
    track,
    context,
    _ingested_at
from {{ ref('stg_spotify__recently_played') }}

{% if is_incremental() %}
where _ingested_at > (select max(_ingested_at) from {{ this }})
{% endif %}
