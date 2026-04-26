{{
    config(
        materialized='incremental',
        unique_key='activity_id',
        merge_update_columns=[
            'activity_name',
            'location_name',
            'start_time_local',
            'start_time_gmt',
            'activity_date',
            'activity_type',
            'hr_zones',
            'split_summaries',
            'performance',
            'gps',
            'weather',
            'sleep',
            'athletic_context',
            '_ingested_at'
        ],
        partition_by={
            'field': 'activity_date',
            'data_type': 'date',
            'granularity': 'month'
        },
        cluster_by=['activity_type'],
        tags=['garmin']
    )
}}

select *
from {{ ref('stg_hub__running_activities') }}

{% if is_incremental() %}
    where _ingested_at > (select max(_ingested_at) from {{ this }})
{% endif %}
