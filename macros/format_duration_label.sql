{% macro format_duration_label(seconds_col, include_seconds=false) %}
    {% if include_seconds %}
        concat(
            cast(DIV(cast(round({{ seconds_col }}) AS int64), 60) AS string),
            ':',
            format('%02d', MOD(cast(round({{ seconds_col }}) AS int64), 60))
        )
    {% else %}
    CASE
        WHEN {{ seconds_col }} >= 3600
            THEN concat(
                cast(DIV(cast(round({{ seconds_col }}) AS int64), 3600) AS string),
                'h ',
                format('%02d', DIV(MOD(cast(round({{ seconds_col }}) AS int64), 3600), 60))
            )
        ELSE concat(
            cast(DIV(cast(round({{ seconds_col }}) AS int64), 60) AS string),
            ' min'
        )
    END
    {% endif %}
{% endmacro %}
