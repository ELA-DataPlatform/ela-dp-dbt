{% macro seconds_to_minutes(seconds, decimal_places=none) %}
    {% if decimal_places is none %}
        {{ seconds }} / 60.0
    {% else %}
        ROUND({{ seconds }} / 60.0, {{ decimal_places }})
    {% endif %}
{% endmacro %}
