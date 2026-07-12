{% macro speed_mps_to_kmh(speed_mps, decimal_places=2) %}
    ROUND({{ speed_mps }} * 3.6, {{ decimal_places }})
{% endmacro %}
