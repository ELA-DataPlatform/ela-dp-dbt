{{ config(alias='stress_daily', tags=['garmin'],
    partition_by={'field': 'health_date', 'data_type': 'date', 'granularity': 'month'}) }}

SELECT  -- noqa: ST06
    TO_HEX(SHA256(TO_JSON_STRING(STRUCT(health_date, identity_scope, source_user_id)))) AS record_id,
    health_date,
    source_user_id,
    identity_scope,
    source_system,
    source_table,
    source_record_key,
    calendar_date,
    avg_stress_level,
    max_stress_level,
    start_at_utc,
    end_at_utc,
    start_at_local,
    end_at_local,
    quality_status,
    quality_issues,
    _ingested_at,
    _silver_processed_at AS _hub_processed_at,
    'Europe/Paris' AS timezone,
    CURRENT_TIMESTAMP() AS _product_built_at
FROM {{ ref('hub_health_svc__stress_daily') }}
