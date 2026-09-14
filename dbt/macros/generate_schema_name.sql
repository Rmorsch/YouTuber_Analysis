{#
    Use a model's +schema verbatim instead of dbt's default
    "<target_schema>_<custom_schema>" concatenation. Without this, the
    staging/marts configs in dbt_project.yml would build into
    STAGING_STAGING and STAGING_MARTS rather than the STAGING and MARTS
    schemas created by snowflake/setup.sql.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
