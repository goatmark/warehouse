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
        src.id
        , src.date
        , src.dish
        , src.dish_type
        , src.plants
        , src.cost
        , sum(1) over (partition by src.dish order by src.date rows unbounded preceding) dish_counter
    from
        src
    where
        1=1
)

select
    cd.id
    , cd.date
    , cd.dish
    , cd.dish_type
    , cd.plants
    , cd.cost
    , cd.dish_counter
    , case
        when dish_counter = 1 then 'New'
        else 'Repeat'
    end dish_type
from
    cleaned_data as cd
where
    1=1