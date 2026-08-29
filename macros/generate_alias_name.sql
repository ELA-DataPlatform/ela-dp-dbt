{% macro generate_alias_name(custom_alias_name, node) -%}
    {%- if custom_alias_name is not none -%}
        {{ custom_alias_name | trim }}
    {%- elif node.resource_type == 'model'
        and node.package_name == 'ela_dp'
        and (node.name.startswith('dlk_') or node.name.startswith('hub_')) -%}
        {{ node.name.split('__')[-1] | trim }}
    {%- elif node.resource_type == 'model'
        and node.package_name == 'ela_dp'
        and node.name.startswith('pct_webapp_') -%}
        {{ node.name.replace('pct_webapp_', '', 1) | trim }}
    {%- elif node.resource_type == 'model'
        and node.package_name == 'ela_dp'
        and node.name.startswith('pct_data4agent__') -%}
        {{ node.name.split('__')[-1] | trim }}
    {%- else -%}
        {{ node.name }}
    {%- endif -%}
{%- endmacro %}
