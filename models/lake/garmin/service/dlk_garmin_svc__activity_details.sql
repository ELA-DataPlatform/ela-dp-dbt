{{
    config(
        materialized='incremental',
        unique_key='activityId',
        merge_update_columns=[
            'data_type',
            'detailsAvailable',
            'totalMetricsCount',
            'metricsCount',
            'measurementCount',
            'heartRateDTOs',
            'pendingData',
            'metric_descriptors',
            'activity_detail_metrics',
            'geo_polyline',
            '_ingested_at',
        ],
        tags=['garmin'],
        partition_by={
            'field': '_ingested_at',
            'data_type': 'timestamp',
            'granularity': 'month'
        }
    )
}}

SELECT *
FROM {{ ref('dlk_garmin_stg__activity_details') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{% endif %}
