-- cln_recipe_log_flattened_plant.sql
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

-- Flatten by plants (comma-separated list)
, flattened_data as (
    select
        src.*
        , trim(plant) as plant
    from src
    cross join unnest(
        split(
        regexp_replace(coalesce(src.plants, ''), r'\s*,\s*', ',')
        )
    ) as plant
    where 
        1=1
        and trim(plant) != ''
)

select
    *
from
    flattened_data
