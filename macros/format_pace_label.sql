{% macro format_pace_label(pace_min_per_km) %}
    concat(
        cast(DIV(cast(round({{ pace_min_per_km }} * 60) AS int64), 60) AS string),
        "'",
        format('%02d', MOD(cast(round({{ pace_min_per_km }} * 60) AS int64), 60)),
        '"/km'
    )
{% endmacro %}
