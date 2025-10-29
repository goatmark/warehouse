-- rpt_metrics_monthly.sql
{{ config(materialized='table') }}

-- FOR MAINTAINABILITY:
-- All queries in rpt_metrics_xxxx.sql series are identical except for params field

with params as (
    -- Adjust to one value among: 'DAY', 'WEEK', 'MONTH', 'QUARTER', 'YEAR'
  select 'DAY' as grain  
)

, src as (
    select
        src.date
        
        -- Exercise data
        , src.total_workouts
        , src.total_reps
        , src.total_sets
        , src.total_volume
        , src.total_volume_load_lbs
        , src.total_runs
        , src.minutes_run
        , src.miles_run
        , src.calories_burned

        -- Finance Data
        , src.total_revenue
        , src.total_tax
        , src.total_expenses
        , src.categorized_expenses
        , src.uncategorized_expenses

        -- Recipe Data
        , src.total_dishes
        , src.new_dishes
        , src.repeat_dishes
        , src.recipe_cost

        -- Shopping Data
        , src.total_items_purchased
        , src.total_quantity_purchased
        , src.total_spend
        
        -- Weight Data
        , src.total_measurements
        , src.avg_weight
        , src.avg_lean_body_mass
        , src.avg_bmi
    from
        {{ref('rpt_metrics')}} as src
)

, date_vector as (
  select
    d as date
  from
    params p
  cross join unnest(
    case
      when p.grain = 'DAY' then generate_date_array(
        date_trunc(date '2021-10-25', day),
        date_trunc(current_date(), day),
        interval 1 day
      )
      when p.grain = 'WEEK' then generate_date_array(
        date_trunc(date '2021-10-25', week),
        date_trunc(current_date(), week),
        interval 7 day
      )
      when p.grain = 'MONTH' then generate_date_array(
        date_trunc(date '2021-10-25', month),
        date_trunc(current_date(), month),
        interval 1 month
      )
      when p.grain = 'QUARTER' then generate_date_array(
        date_trunc(date '2021-10-25', quarter),
        date_trunc(current_date(), quarter),
        interval 1 quarter
      )
      when p.grain = 'YEAR' then generate_date_array(
        date_trunc(date '2021-10-25', year),
        date_trunc(current_date(), year),
        interval 1 year
      )
    end
  ) as d
)

, src_agg as (
    select
        case p.grain 
            when 'DAY' then date_trunc(dv.date, DAY) 
            when 'WEEK' then date_trunc(dv.date, WEEK) 
            when 'MONTH' then date_trunc(dv.date, MONTH)
            when 'QUARTER' then date_trunc(dv.date, QUARTER)  
            when 'YEAR' then date_trunc(dv.date, YEAR)
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
        date_vector dv
    cross join params as p
    left join src on
        src.date = dv.date
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
    src.date
    
    -- Exercise data
    , src.total_workouts
    , src.total_reps
    , src.total_sets
    , src.total_volume
    , src.total_volume_load_lbs
    , src.total_runs
    , src.minutes_run
    , src.miles_run
    , src.calories_burned

    -- NEW: Unique Exercises
    , ed.unique_exercises

    -- Finance Data
    , src.total_revenue
    , src.total_tax
    , src.total_expenses
    , src.categorized_expenses
    , src.uncategorized_expenses

    -- NEW: Unique Dishes & Plants
    , rd.unique_dishes
    , pd.unique_plants

    -- Recipe Data
    , src.total_dishes
    , src.new_dishes
    , src.repeat_dishes
    , src.recipe_cost

    -- Shopping Data
    , src.total_items_purchased
    , src.total_quantity_purchased
    , src.total_spend
    
    -- Weight Data
    , src.total_measurements
    , src.avg_weight
    , src.avg_lean_body_mass
    , src.avg_bmi
from
    src_agg as src
left join exercise_data ed on
    src.date = ed.date
left join recipe_data rd on
    src.date = rd.date
left join plant_data pd on
    src.date = pd.date