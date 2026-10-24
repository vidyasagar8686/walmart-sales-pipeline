{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}

    {#- Production job (target name "prod"): use the real schema names -#}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}

    {#- Development (IDE): prefix with the developer's schema, e.g. DBT_DEV_MART -#}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
