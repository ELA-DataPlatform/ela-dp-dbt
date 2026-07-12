{% macro milliseconds_to_minutes(milliseconds) %}
    CAST({{ milliseconds }} / 60000 AS INT64)
{% endmacro %}
