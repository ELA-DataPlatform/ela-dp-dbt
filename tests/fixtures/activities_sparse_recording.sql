SELECT
    42 AS activityid,
    TIMESTAMP '2026-01-01 12:00:00+00' AS _ingested_at,
    [
        STRUCT(0 AS metrics_index, 'directTimestamp' AS metric_key, 'gmt' AS unit_key, 0.0 AS unit_factor),
        STRUCT(1 AS metrics_index, 'directSpeed' AS metric_key, 'mps' AS unit_key, 0.1 AS unit_factor),
        STRUCT(2 AS metrics_index, 'directHeartRate' AS metric_key, 'bpm' AS unit_key, 1.0 AS unit_factor),
        STRUCT(3 AS metrics_index, 'directStrideLength' AS metric_key, 'centimeter' AS unit_key, 1.0 AS unit_factor),
        STRUCT(4 AS metrics_index, 'directVerticalOscillation' AS metric_key, 'centimeter' AS unit_key, 1.0 AS unit_factor)
    ] AS metric_descriptors,
    [
        STRUCT(0 AS sample_index, [
            STRUCT(0 AS metric_index, '1767268800000' AS source_text, 1767268800000.0 AS metric_value),
            STRUCT(1 AS metric_index, 'null' AS source_text, CAST(NULL AS FLOAT64) AS metric_value),
            STRUCT(2 AS metric_index, '125' AS source_text, 125.0 AS metric_value),
            STRUCT(3 AS metric_index, '110' AS source_text, 110.0 AS metric_value),
            STRUCT(4 AS metric_index, '8.5' AS source_text, 8.5 AS metric_value)
        ] AS metric_values),
        STRUCT(1 AS sample_index, [
            STRUCT(0 AS metric_index, '1767268800000' AS source_text, 1767268800000.0 AS metric_value),
            STRUCT(1 AS metric_index, '0' AS source_text, 0.0 AS metric_value),
            STRUCT(2 AS metric_index, '130' AS source_text, 130.0 AS metric_value),
            STRUCT(3 AS metric_index, '120' AS source_text, 120.0 AS metric_value),
            STRUCT(4 AS metric_index, '9' AS source_text, 9.0 AS metric_value)
        ] AS metric_values)
    ] AS indexed_metric_samples
