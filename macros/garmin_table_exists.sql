{% macro garmin_table_exists(table_name) %}
    {%- set relation = adapter.get_relation(
        database=target.project,
        schema='dp_lake_garmin_' ~ target.name,
        identifier=table_name
    ) -%}
    {{ return(relation is not none) }}
{% endmacro %}
