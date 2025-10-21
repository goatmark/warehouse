-- cln_recipe_log_flattened_type.sql
{{ config(materialized='view') }}

-- Import from cln_recipe_log.sql model
with src as (
    select
        *
    from
        {{ref('cln_recipe_log')}}
    where
        1=1
)

-- Flatten by dish type (comma-separated lists)
, flattened_data as (
    select
        src.date
        , src.dish
        , src.cost
        , trim(type) as type
    from src
    cross join unnest(
        split(
        regexp_replace(coalesce(src.dish_type, ''), r'\s*,\s*', ',')
        )
    ) as type
    where 
        1=1
        and trim(type) != ''
)

select
    *
from
    flattened_data
