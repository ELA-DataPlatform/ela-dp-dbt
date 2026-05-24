{% macro dayofweek_to_french_letter(date_col) %}
    CASE extract(DAYOFWEEK FROM {{ date_col }})
        WHEN 1 THEN 'D'
        WHEN 2 THEN 'L'
        WHEN 3 THEN 'M'
        WHEN 4 THEN 'M'
        WHEN 5 THEN 'J'
        WHEN 6 THEN 'V'
        WHEN 7 THEN 'S'
    END
{% endmacro %}
