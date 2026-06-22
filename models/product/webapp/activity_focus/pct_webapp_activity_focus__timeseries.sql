{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            'field': 'activity_date',
            'data_type': 'date',
            'granularity': 'month'
        },
        cluster_by=['activity_id'],
        tags=['garmin', 'webapp']
    )
}}

SELECT
    hub.activity_id,
    hub.activity_date,

    ts.timestamp_gmt,
    ts.elapsed_s,
    ts.cum_distance_m,
    ts.heart_rate_bpm,
    ts.cadence_spm,
    ts.latitude,
    ts.longitude,
    ts.power_w,
    ts.vertical_oscillation_mm,
    ts.vertical_ratio,
    ts.ground_contact_ms,
    ts.stride_length_m,
    ts.stamina_available,
    ts.stamina_potential,
    ts.air_temp_celsius,
    hub._ingested_at,
    ROUND(ts.cum_distance_m / 1000.0, 3) AS cum_distance_km,
    SAFE_DIVIDE(1000.0, ts.speed_m_per_s) / 60.0 AS pace_min_per_km,
    SAFE_DIVIDE(1000.0, ts.grade_adj_speed_m_per_s) / 60.0 AS gap_min_per_km,

    COALESCE(ts.corrected_elevation_m, ts.elevation_m) AS elevation_m

FROM {{ ref('svc_hub__master_running_activities') }} AS hub
CROSS JOIN UNNEST(hub.timeseries) AS ts

{% if is_incremental() %}
    WHERE
        hub._ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
        AND ts.timestamp_gmt > TIMESTAMP('1971-01-01')
{% else %}
    WHERE ts.timestamp_gmt > TIMESTAMP('1971-01-01')
{% endif %}
