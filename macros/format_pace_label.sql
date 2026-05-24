{% macro format_pace_label(pace_min_per_km) %}
    concat(
        cast(cast({{ pace_min_per_km }} AS int64) AS string),
        "'",
        format('%02d', cast(round(({{ pace_min_per_km }} - cast({{ pace_min_per_km }} AS int64)) * 60) AS int64)),
        '"/km'
    )
{% endmacro %}
