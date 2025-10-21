-- cln_recipe_log.sql
{{ config(materialized='view') }}

-- Import file and cast
with src as (
    select
        cast(ID as string)          as id
        , cast(Date as date)        as date
        , cast(Dish as string)      as dish
        , cast(Dish_Type as string) as dish_type
        , cast(Plants as string)    as plants
        , cast(Cost as numeric)     as cost
    from
        {{source('warehouse', 'recipe_log')}}
    where
        1=1
)

, cleaned_data as (
    select
        *
    from
        src
    where
        1=1
)

select
    * 
from
    cleaned_data
where
    1=1