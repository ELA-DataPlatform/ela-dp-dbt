
{{
    config(
        materialized='incremental',
        unique_key=['deviceId', 'date', 'startTimestampGMT'],
        tags=['garmin'],
        partition_by={
            'field': '_ingested_at',
            'data_type': 'timestamp',
            'granularity': 'month'
        }
    )
}}

select *
from {{ ref('stg_garmin__all_day_events') }}

{% if is_incremental() %}
    where _ingested_at > (select max(_ingested_at) from {{ this }})
{% endif %}
