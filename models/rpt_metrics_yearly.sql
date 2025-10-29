-- rpt_metrics_yearly.sql
{{ config(materialized='table') }}

-- FOR MAINTAINABILITY:
-- All queries in rpt_metrics_xxxx.sql series are identical except for params field

with params as (
    -- Adjust to one value among: 'DAY', 'WEEK', 'MONTH', 'QUARTER', 'YEAR'
  select 'YEAR' as grain  
)

, src_agg as (
    select
        case p.grain 
            when 'DAY' then date_trunc(src.date, DAY) 
            when 'WEEK' then date_trunc(src.date, WEEK) 
            when 'MONTH' then date_trunc(src.date, MONTH)
            when 'QUARTER' then date_trunc(src.date, QUARTER)  
            when 'YEAR' then date_trunc(src.date, YEAR)
        end as date

        -- Exercise data
        , sum(src.total_workouts) as total_workouts
        , sum(src.total_reps) as total_reps
        , sum(src.total_sets) as total_sets
        , sum(src.total_volume) as total_volume
        , sum(src.total_volume_load_lbs) as total_volume_load_lbs
        , sum(src.total_runs) as total_runs
        , sum(src.minutes_run) as minutes_run
        , sum(src.miles_run) as miles_run
        , sum(src.calories_burned) as calories_burned

        -- Finance Data
        , sum(src.total_revenue) as total_revenue
        , sum(src.total_tax) as total_tax
        , sum(src.total_expenses) as total_expenses
        , sum(src.categorized_expenses) as categorized_expenses
        , sum(src.uncategorized_expenses) as uncategorized_expenses

        -- Recipe Data
        , sum(src.total_dishes) as total_dishes
        , sum(src.new_dishes) as new_dishes
        , sum(src.repeat_dishes) as repeat_dishes
        , sum(src.recipe_cost) as recipe_cost

        -- Shopping Data
        , sum(src.total_items_purchased) as total_items_purchased
        , sum(src.total_quantity_purchased) as total_quantity_purchased
        , sum(src.total_spend) as total_spend
        
        -- Weight Data
        , sum(src.total_measurements) as total_measurements
        , avg(src.avg_weight) as avg_weight
        , avg(src.avg_lean_body_mass) as avg_lean_body_mass
        , avg(src.avg_bmi) as avg_bmi
    from
        {{ref('rpt_metrics')}} as src
    cross join params as p
    where
        1=1
    group by
        1
)

, exercise_data as (
    select
        case p.grain 
            when 'DAY' then date_trunc(ed.date, DAY) 
            when 'WEEK' then date_trunc(ed.date, WEEK) 
            when 'MONTH' then date_trunc(ed.date, MONTH)
            when 'QUARTER' then date_trunc(ed.date, QUARTER)  
            when 'YEAR' then date_trunc(ed.date, YEAR)
        end as date
        , count(distinct ed.exercise_label) unique_exercises
    from
        {{ref('cln_exercise_log')}} as ed
    cross join params as p
    where
        1=1
    group by
        1 
)

, recipe_data as (
    select
        case p.grain 
            when 'DAY' then date_trunc(rd.date, DAY) 
            when 'WEEK' then date_trunc(rd.date, WEEK) 
            when 'MONTH' then date_trunc(rd.date, MONTH)
            when 'QUARTER' then date_trunc(rd.date, QUARTER)  
            when 'YEAR' then date_trunc(rd.date, YEAR)
        end as date
        , count(distinct rd.dish) unique_dishes
    from
        {{ref('cln_recipe_log')}} as rd
    cross join params as p
    where
        1=1
    group by
        1 
)

, plant_data as (
    select
        case p.grain 
            when 'DAY' then date_trunc(rd.date, DAY) 
            when 'WEEK' then date_trunc(rd.date, WEEK) 
            when 'MONTH' then date_trunc(rd.date, MONTH)
            when 'QUARTER' then date_trunc(rd.date, QUARTER)  
            when 'YEAR' then date_trunc(rd.date, YEAR)
        end as date
        , count(distinct rd.plant) unique_plants
    from
        {{ref('cln_recipe_log_flattened_plant')}} as rd
    cross join params as p
    where
        1=1
    group by
        1 
)

select
    src_agg.date
    
    -- Exercise data
    , src_agg.total_workouts
    , src_agg.total_reps
    , src_agg.total_sets
    , src_agg.total_volume
    , src_agg.total_volume_load_lbs
    , src_agg.total_runs
    , src_agg.minutes_run
    , src_agg.miles_run
    , src_agg.calories_burned

    -- NEW: Unique Exercises
    , ed.unique_exercises

    -- Finance Data
    , src_agg.total_revenue
    , src_agg.total_tax
    , src_agg.total_expenses
    , src_agg.categorized_expenses
    , src_agg.uncategorized_expenses

    -- NEW: Unique Dishes & Plants
    , rd.unique_dishes
    , pd.unique_plants

    -- Recipe Data
    , src_agg.total_dishes
    , src_agg.new_dishes
    , src_agg.repeat_dishes
    , src_agg.recipe_cost

    -- Shopping Data
    , src_agg.total_items_purchased
    , src_agg.total_quantity_purchased
    , src_agg.total_spend
    
    -- Weight Data
    , src_agg.total_measurements
    , src_agg.avg_weight
    , src_agg.avg_lean_body_mass
    , src_agg.avg_bmi
from
    src_agg as src_agg
left join exercise_data as ed on
    src_agg.date = ed.date
left join recipe_data as rd on
    src_agg.date = rd.date
left join plant_data as pd on
    src_agg.date = pd.date