{% macro bigquery__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates=none) -%}
    {# Keep the dbt default outside canonical lake reconciliation. #}
    {% if not config.get('meta', {}).get('reconcile_canonical_rows', false) or not unique_key %}
        {{ return(dbt.default__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates)) }}
    {% endif %}

    {% set keys = [unique_key] if unique_key is string else unique_key %}
    {% set predicates = [] if incremental_predicates is none else [] + incremental_predicates %}
    {% set update_columns = get_merge_update_columns(
        config.get('merge_update_columns'), config.get('merge_exclude_columns'), dest_columns
    ) %}
    {# A JSON tuple preserves nulls without sentinels and keeps an equijoin for BigQuery. #}
    {% set source_keys = [] %}
    {% set target_keys = [] %}
    {% for key in keys %}
        {% do source_keys.append('DBT_INTERNAL_SOURCE.' ~ adapter.quote(key) ~ ' AS ' ~ adapter.quote(key)) %}
        {% do target_keys.append('DBT_INTERNAL_DEST.' ~ adapter.quote(key) ~ ' AS ' ~ adapter.quote(key)) %}
    {% endfor %}
    {% do predicates.append(
        'TO_JSON_STRING(STRUCT(' ~ source_keys | join(', ')
        ~ ')) = TO_JSON_STRING(STRUCT(' ~ target_keys | join(', ') ~ '))'
    ) %}

    {{ config.get('sql_header') or '' }}

    MERGE INTO {{ target }} AS DBT_INTERNAL_DEST
    USING {{ source }} AS DBT_INTERNAL_SOURCE
        ON ({{ predicates | join(') AND (') }})
    WHEN MATCHED AND (
        {% for column in update_columns %}
            TO_JSON_STRING(DBT_INTERNAL_SOURCE.{{ column }})
            IS DISTINCT FROM TO_JSON_STRING(DBT_INTERNAL_DEST.{{ column }})
            {% if not loop.last %} OR {% endif %}
    {% endfor %}
    ) THEN UPDATE SET
        {% for column in update_columns %}
        {{ column }} = DBT_INTERNAL_SOURCE.{{ column }}{% if not loop.last %},{% endif %}
    {% endfor %}
    WHEN NOT MATCHED THEN INSERT ({{ get_quoted_csv(dest_columns | map(attribute='name')) }})
    VALUES ({{ get_quoted_csv(dest_columns | map(attribute='name')) }})
{%- endmacro %}
