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
    {{ meters_to_kilometers('ts.cum_distance_m', 3) }} AS cum_distance_km,
    {{ speed_mps_to_pace_min_per_km('ts.speed_m_per_s') }} AS pace_min_per_km,
    {{ speed_mps_to_pace_min_per_km('ts.grade_adj_speed_m_per_s') }} AS gap_min_per_km,

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
