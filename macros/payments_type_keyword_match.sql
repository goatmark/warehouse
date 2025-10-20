{% macro payments_type_keyword_match(col) -%}
(
  {{ col }} like '%online transfer%' or
  {{ col }} like '%edward jones%' or
  {{ col }} like '%jpmorgan chase%' or
  {{ col }} like '%fedwire%' or
  {{ col }} like '%automatic payment%'
)
{%- endmacro %}