{{
    config(
        materialized='incremental',
        unique_key=['activity_id', 'timestamp_gmt'],
        merge_update_columns=[
            'activity_date',
            'activity_type',
            'elapsed_s',
            'duration_s',
            'moving_duration_s',
            'cum_distance_m',
            'speed_m_per_s',
            'heart_rate_bpm',
            'cadence_spm',
            'fractional_cadence',
            'body_battery',
            'latitude',
            'longitude',
            '_ingested_at'
        ],
        partition_by={
            'field': 'activity_date',
            'data_type': 'date',
            'granularity': 'month'
        },
        cluster_by=['activity_id'],
        tags=['garmin']
    )
}}

select *
from {{ ref('stg_hub__running_activity_seconds') }}

{% if is_incremental() %}
    where _ingested_at > (select max(_ingested_at) from {{ this }})
{% endif %}
