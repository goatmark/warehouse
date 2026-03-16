-- cln_recipes.sql
{{ config(materialized='view') }}

with src as (
    select
        cast(ID as string)                      as recipe_id
        , cast(Name as string)                  as dish_name
        , cast(Source as string)                as recipe_source
        , cast(Cuisine as string)               as cuisine
        , cast(Dish_Type as string)             as dish_type
        , cast(Protein_Options as string)       as protein_options
        , cast(Unique_Plant_Count as int64)     as plant_count
        , cast(Plant_List as string)            as plant_list
        , cast(Last_Made as date)               as last_made
        , cast(Times_Made as int64)             as times_made
        , cast(created_at as timestamp)         as created_at
        , cast(updated_at as timestamp)         as updated_at
    from
        {{source('warehouse', 'recipes')}} as src
    where
        1=1
)

-- Get flattened table of dish, plant for later comparing, filtering, and aggregation
, plants_by_recipe as (
    select
        src.dish_name
        , trim(plant) as plant
    from src
    cross join unnest(
        split(
        regexp_replace(coalesce(src.plant_list, ''), r'\s*,\s*', ',')
        )
    ) as plant
    where 
        1=1
        and trim(plant) != ''
)

-- Identify total number of unique plants in recipes prepared in current month
, plants_this_month as (
    select
        pbr.plant as plant_list
    from
        plants_by_recipe pbr
    left join src on
        src.dish_name = pbr.dish_name
    where
        1=1
        and date_trunc(src.last_made, MONTH) = date_trunc(current_date, MONTH)
    group by
        1
)

-- Identify plants found in Yogurt Bowls (e.g. to keep Yogurt Bowls, Parfaits, Desserts from being high)
, plants_in_breakfast as (
    select
        plant as plant_list
    from
        plants_by_recipe
    where
        1=1
        and (1=0
        or dish_name = 'Yogurt Bowl'
        )
    group by
        1
)

-- Count all new plants in dish (excluding those in Yogurt Bowl or consumed this month already)
, new_plants_per_dish as (
    select
        pbr.dish_name
        , count(*) new_plants
    from
        plants_by_recipe as pbr
    where
        1=1
        and pbr.plant not in (select plant_list from plants_this_month)
        and pbr.plant not in (select plant_list from plants_in_breakfast)
    group by
        pbr.dish_name
)

select
    src.recipe_id
    , src.dish_name
    , src.recipe_source
    , src.cuisine
    , src.dish_type
    , src.protein_options
    , src.last_made
    , src.times_made
    , src.plant_count
    , nppd.new_plants
    , src.created_at
    , src.updated_at
from
    src
left join new_plants_per_dish nppd on
    src.dish_name = nppd.dish_name
where
    1=1
order by
    new_plants desc nulls last