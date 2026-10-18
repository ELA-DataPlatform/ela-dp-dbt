{% macro garmin_health_qualify_samples(parse_timestamp, include_reading_confidence=false) -%}
parsed_points AS (
    SELECT
        *,
        {{ parse_timestamp('source_timestamp') }} AS observed_at_utc,
        {{ parse_timestamp('source_end_timestamp') }} AS end_at_utc,
        DATETIME({{ parse_timestamp('source_local_timestamp') }}) AS observed_at_local,
        TO_HEX(SHA256(TO_JSON_STRING(STRUCT(
            source_table,
            source_record_key,
            source_array,
            metric_name,
            sample_index
        )))) AS sample_key
    FROM raw_points
),

qualified_points AS (
    SELECT
        *,
        source_value IS NOT NULL
        AND IF(_min_inclusive, source_value >= _min_value, source_value > _min_value)
        AND (_max_value IS NULL OR source_value <= _max_value) AS _value_in_domain,
        ARRAY_CONCAT(
            IF(health_date IS NULL, ['missing_health_date'], ARRAY<STRING>[]),
            IF(identity_scope != 'account' AND source_user_id IS NULL,
                ['missing_source_user_id'], ARRAY<STRING>[]),
            IF(observed_at_utc IS NULL, ['invalid_timestamp'], ARRAY<STRING>[]),
            IF(source_local_timestamp IS NOT NULL AND observed_at_local IS NULL,
                ['invalid_local_timestamp'], ARRAY<STRING>[]),
            IF(timestamp_role = 'start' AND end_at_utc IS NULL,
                ['invalid_interval_end'], ARRAY<STRING>[]),
            IF(end_at_utc < observed_at_utc, ['invalid_interval_order'], ARRAY<STRING>[]),
            IF(source_value IS NOT NULL AND NOT (
                IF(_min_inclusive, source_value >= _min_value, source_value > _min_value)
                AND (_max_value IS NULL OR source_value <= _max_value)
            ), ['value_out_of_domain'], ARRAY<STRING>[])
        ) AS _invalid_issues
    FROM parsed_points
)

SELECT
    health_date,
    source_user_id,
    identity_scope,
    source_table,
    source_record_key,
    sample_key,
    sample_index,
    source_array,
    metric_name,
    metric_unit,
    observed_at_utc,
    observed_at_local,
    end_at_utc,
    timestamp_role,
    source_value,
    {% if include_reading_confidence %}
    reading_confidence,
    {% endif %}
    source_timestamp,
    source_end_timestamp,
    source_local_timestamp,
    source_sample,
    _ingested_at,
    IF(_value_in_domain, source_value, NULL) AS measurement_value,
    CASE
        WHEN ARRAY_LENGTH(_invalid_issues) > 0 THEN 'invalid'
        WHEN source_value IS NULL THEN 'partial'
        ELSE 'valid'
    END AS quality_status,
    ARRAY_CONCAT(
        _invalid_issues,
        IF(source_value IS NULL, ['missing_value'], ARRAY<STRING>[])
    ) AS quality_issues
FROM qualified_points
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY sample_key
    ORDER BY _ingested_at DESC, source_sample ASC
) = 1
{%- endmacro %}
