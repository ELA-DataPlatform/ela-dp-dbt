{% macro format_day_label(date_col) %}
    concat(
        cast(extract(DAY FROM {{ date_col }}) AS string),
        '/',
        cast(extract(MONTH FROM {{ date_col }}) AS string)
    )
{% endmacro %}
