
{{
    config(
        materialized='incremental',
        unique_key=['userId', 'calendarDate'],
        tags=['garmin'],
        partition_by={
            'field': '_ingested_at',
            'data_type': 'timestamp',
            'granularity': 'month'
        }
    )
}}

select *
from {{ ref('stg_garmin__race_predictions') }}

{% if is_incremental() %}
    where _ingested_at > (select max(_ingested_at) from {{ this }})
{% endif %}
