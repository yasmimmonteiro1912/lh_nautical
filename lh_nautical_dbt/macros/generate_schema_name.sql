{% macro generate_schema_name(custom_schema_name, node) -%}
    {# 
       Overriding the default dbt behavior to prevent schema concatenation.
       If a custom schema (e.g., 'gold' or 'silver') is defined in the dbt_project.yml, 
       this macro forces dbt to use it directly instead of appending it to the target schema.
    #}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}