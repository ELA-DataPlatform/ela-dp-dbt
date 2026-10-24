{{
    config(
        materialized='incremental',
        unique_key='activityId',
        on_schema_change='append_new_columns',
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
            'indexed_metric_samples',
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

-- Reconcile every canonical staging row, including late records and corrections.
SELECT *
FROM {{ ref('dlk_garmin_stg__activity_details') }}
