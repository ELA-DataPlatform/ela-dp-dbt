{% macro meters_to_kilometers(meters, decimal_places=none) %}
    {% if decimal_places is none %}
        {{ meters }} / 1000.0
    {% else %}
        ROUND({{ meters }} / 1000.0, {{ decimal_places }})
    {% endif %}
{% endmacro %}
