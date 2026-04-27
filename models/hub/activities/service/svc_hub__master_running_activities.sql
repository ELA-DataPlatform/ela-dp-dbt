{{
    config(
        materialized='incremental',
        unique_key='activity_id',
        merge_update_columns=[
            'activity_uuid',
            'activity_name',
            'activity_description',
            'location_name',
            'start_time_local',
            'start_time_gmt',
            'end_time_gmt',
            'activity_date',
            'activity_type',
            'device_id',
            'manufacturer',
            'is_pr',
            'is_favorite',
            'is_manual',
            'is_elevation_corrected',
            'event_type',
            'performance',
            'fastest_splits',
            'gps',
            'hr_zones',
            'power_zones',
            'laps',
            'typed_splits',
            'split_summaries',
            'timeseries',
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

SELECT *
FROM {{ ref('stg_hub__master_running_activities') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}
