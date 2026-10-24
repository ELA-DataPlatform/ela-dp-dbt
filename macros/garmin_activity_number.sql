{% macro garmin_activity_number(value, minimum=none, maximum=none) -%}
    IF(
        NOT IS_NAN(SAFE_CAST({{ value }} AS FLOAT64))
        AND NOT IS_INF(SAFE_CAST({{ value }} AS FLOAT64))
        {% if minimum is not none %}
        AND SAFE_CAST({{ value }} AS FLOAT64) >= {{ minimum }}
    {% endif %}
    {% if maximum is not none %}
        AND SAFE_CAST({{ value }} AS FLOAT64) <= {{ maximum }}
    {% endif %}
        , SAFE_CAST({{ value }} AS FLOAT64), NULL
    )
{%- endmacro %}
