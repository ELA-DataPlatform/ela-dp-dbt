{{ config(materialized='view') }}

-- Assemble one-row-per-activity services only. Repeated children never multiply facts.
SELECT
    a.activity_id,
    a.activity_key,
    a.activity_date,
    a.identity,
    a.sport,
    a.timing,
    a.performance,
    a.running,
    a.cycling,
    a.strength,
    a.gps,
    a.flags,
    a.event_type,
    a.source_values,
    a._ingested_at,
    a.quality_status,
    a.quality_issues,
    STRUCT(
        COALESCE(r.timeseries, []) AS timeseries,
        r.route,
        COALESCE(r.metric_descriptors, []) AS metric_descriptors,
        r.source_details_available,
        r.source_total_metrics_count,
        r.source_metrics_count,
        r.source_measurement_count,
        'source_returned_samples' AS resolution,
        r.source_values
    ) AS recordings,
    STRUCT(
        COALESCE(s.laps, []) AS laps,
        COALESCE(s.typed_splits, []) AS typed_splits,
        COALESCE(s.split_summaries, []) AS split_summaries
    ) AS breakdown,
    STRUCT(
        COALESCE(z.heart_rate, []) AS heart_rate,
        COALESCE(z.power, []) AS power
    ) AS zones,
    COALESCE(e.exercise_sets, []) AS exercise_sets,
    (SELECT AS STRUCT w.* EXCEPT (activity_id, _silver_processed_at)) AS weather,
    STRUCT(h.sleep, h.athletic_context, h.association, h.provenance) AS health_context,
    STRUCT(
        STRUCT(a.quality_status AS status, a.quality_issues AS issues) AS activity,
        STRUCT(r.quality_status AS status, r.quality_issues AS issues) AS recording,
        STRUCT(s.quality_status AS status, s.quality_issues AS issues) AS splits,
        STRUCT(z.quality_status AS status, z.quality_issues AS issues) AS zones,
        STRUCT(e.quality_status AS status, e.quality_issues AS issues) AS exercise_sets,
        STRUCT(w.quality_status AS status, w.quality_issues AS issues) AS weather
    ) AS quality,
    STRUCT(
        r.activity_id IS NOT NULL AS recording_response_present,
        COALESCE(ARRAY_LENGTH(r.timeseries), 0) > 0 AS has_timeseries,
        COALESCE(ARRAY_LENGTH(r.route.polyline), 0) > 0 AS has_route,
        s.activity_id IS NOT NULL AS splits_response_present,
        COALESCE(ARRAY_LENGTH(s.laps), 0) > 0 AS has_laps,
        COALESCE(z.heart_rate_response_present, FALSE) AS heart_rate_zones_response_present,
        COALESCE(ARRAY_LENGTH(z.heart_rate), 0) > 0 AS has_heart_rate_zones,
        COALESCE(ARRAY_LENGTH(z.power), 0) > 0 AS has_power_zones,
        e.activity_id IS NOT NULL AS exercise_sets_response_present,
        COALESCE(ARRAY_LENGTH(e.exercise_sets), 0) > 0 AS has_exercise_sets,
        w.activity_id IS NOT NULL AS weather_response_present,
        COALESCE(r.invalid_sample_count, 0) AS invalid_sample_count,
        COALESCE(r.partial_sample_count, 0) AS partial_sample_count,
        COALESCE(r.invalid_route_point_count, 0) AS invalid_route_point_count
    ) AS availability,
    ARRAY(
        SELECT AS STRUCT p.*
        FROM
            UNNEST([
                STRUCT('activities' AS source_table, a._ingested_at AS source_ingested_at),
                STRUCT('activity_details' AS source_table, r._ingested_at AS source_ingested_at),
                STRUCT('activity_splits' AS source_table, s._ingested_at AS source_ingested_at),
                STRUCT('activity_hr_zones' AS source_table, z.heart_rate_ingested_at AS source_ingested_at),
                STRUCT('activity_exercise_sets' AS source_table, e._ingested_at AS source_ingested_at),
                STRUCT('activity_weather' AS source_table, w._ingested_at AS source_ingested_at)
            ]) AS p
        WHERE p.source_ingested_at IS NOT NULL
        ORDER BY p.source_table
    ) AS provenance
FROM {{ ref('hub_activities_svc__fact_activity') }} AS a
LEFT JOIN {{ ref('hub_activities_svc__activity_recording') }} AS r ON a.activity_id = r.activity_id
LEFT JOIN {{ ref('hub_activities_svc__activity_splits') }} AS s ON a.activity_id = s.activity_id
LEFT JOIN {{ ref('hub_activities_svc__activity_zones') }} AS z ON a.activity_id = z.activity_id
LEFT JOIN {{ ref('hub_activities_svc__activity_exercise_sets') }} AS e ON a.activity_id = e.activity_id
LEFT JOIN {{ ref('hub_activities_svc__activity_weather') }} AS w ON a.activity_id = w.activity_id
LEFT JOIN {{ ref('hub_activities_stg__activity_health_context') }} AS h ON a.activity_id = h.activity_id
