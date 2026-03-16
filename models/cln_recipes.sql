-- cln_recipes.sql
{{ config(materialized='view') }}

-- select * from {{source('warehouse', 'recipes')}}

select 1 as query_value