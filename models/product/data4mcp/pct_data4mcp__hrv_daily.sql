{{ config(alias='hrv_daily', tags=['garmin'],
    partition_by={'field': 'health_date', 'data_type': 'date', 'granularity': 'month'}) }}

SELECT  -- noqa: ST06
    TO_HEX(SHA256(TO_JSON_STRING(STRUCT(health_date, identity_scope, source_user_id)))) AS record_id,
    health_date,
    source_user_id,
    identity_scope,
    source_system,
    source_table,
    source_record_key,
    hrv_calendar_date,
    hrv_status,
    hrv_feedback_phrase,
    hrv_last_night_avg AS hrv_last_night_avg_ms,
    hrv_last_night_5min_high AS hrv_last_night_5min_high_ms,
    hrv_weekly_avg AS hrv_weekly_avg_ms,
    hrv_baseline_low_upper AS hrv_baseline_low_upper_ms,
    hrv_baseline_balanced_low AS hrv_baseline_balanced_low_ms,
    hrv_baseline_balanced_upper AS hrv_baseline_balanced_upper_ms,
    hrv_baseline_marker_value AS hrv_baseline_marker_value_ms,
    quality_status,
    quality_issues,
    _ingested_at,
    _silver_processed_at AS _hub_processed_at,
    'Europe/Paris' AS timezone,
    CURRENT_TIMESTAMP() AS _product_built_at
FROM {{ ref('hub_health_svc__hrv_daily') }}
