SELECT
    43 AS activityid,
    TIMESTAMP '2026-01-01 12:00:00+00' AS _ingested_at,
    [
        STRUCT(0 AS metrics_index, 'directTimestamp' AS metric_key, 'gmt' AS unit_key, 0.0 AS unit_factor),
        STRUCT(1 AS metrics_index, 'directSpeed' AS metric_key, 'kmh' AS unit_key, 1.0 AS unit_factor)
    ] AS metric_descriptors,
    [STRUCT(0 AS sample_index, [
        STRUCT(0 AS metric_index, '1767268800000' AS source_text, 1767268800000.0 AS metric_value),
        STRUCT(1 AS metric_index, '12' AS source_text, 12.0 AS metric_value)
    ] AS metric_values)] AS indexed_metric_samples
