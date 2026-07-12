{% macro minutes_per_km_to_seconds_per_km(minutes_per_km, decimal_places=0) %}
    ROUND({{ minutes_per_km }} * 60, {{ decimal_places }})
{% endmacro %}
