{% macro garmin_health_timestamp(value) -%}
    CASE
        WHEN REGEXP_CONTAINS(CAST({{ value }} AS STRING), r'^-?[0-9]+$')
            THEN SAFE.TIMESTAMP_MILLIS(SAFE_CAST({{ value }} AS INT64))
        ELSE SAFE_CAST(CAST({{ value }} AS STRING) AS TIMESTAMP)
    END
{%- endmacro %}
