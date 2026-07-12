{% macro speed_mps_to_pace_min_per_km(speed_mps) %}
    SAFE_DIVIDE(1000.0, {{ speed_mps }}) / 60.0
{% endmacro %}
