{#
    3-way schema routing.

    By default dbt concatenates the target schema with a model's custom
    schema (e.g. dwh_reporting_dwh_finance_business). We override that so a
    model's custom +schema is used VERBATIM:

      - no custom schema      -> target.schema (default: dwh_reporting)
      - custom schema present -> that schema, trimmed

    This lets models land in dwh_finance_business / dwh_finance_personal /
    dwh_reporting without any target-schema prefix.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
