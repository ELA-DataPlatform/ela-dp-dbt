{% macro minutes_to_label(minutes) %}
    CASE
        WHEN {{ minutes }} >= 60
            THEN concat(
                cast(DIV(cast({{ minutes }} AS int64), 60) AS string),
                'h',
                format('%02d', mod(cast({{ minutes }} AS int64), 60))
            )
        ELSE concat(cast(cast({{ minutes }} AS int64) AS string), 'min')
    END
{% endmacro %}
