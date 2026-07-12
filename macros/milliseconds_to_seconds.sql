{% macro milliseconds_to_seconds(milliseconds, decimal_places=0) %}
    ROUND({{ milliseconds }} / 1000.0, {{ decimal_places }})
{% endmacro %}
