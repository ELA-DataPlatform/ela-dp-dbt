
{{
    config(
        materialized='incremental',
        unique_key=['userProfileId', 'date'],
        tags=['garmin'],
        partition_by={
            'field': '_ingested_at',
            'data_type': 'timestamp',
            'granularity': 'month'
        }
    )
}}

select *
from {{ ref('stg_garmin__stats_and_body') }}

{% if is_incremental() %}
    where _ingested_at > (select max(_ingested_at) from {{ this }})
{% endif %}
