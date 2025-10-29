-- rpt_metrics_monthly.sql
{{ config(materialized='table') }}

-- FOR MAINTAINABILITY:
-- All queries in rpt_metrics series are identical except for params field
-- If future edits needed: make all changes in rpt_metrics_daily.sql and extrapolate,
-- changing only grain across models

with params as (
    -- Adjust to one value among: 'DAY', 'WEEK', 'MONTH', 'QUARTER', 'YEAR'
  select 'MONTH' as grain  
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

, exercise_data as (
    select  
        case p.grain 
            when 'DAY' then date_trunc(ed.date, DAY) 
            when 'WEEK' then date_trunc(ed.date, WEEK) 
            when 'MONTH' then date_trunc(ed.date, MONTH)
            when 'QUARTER' then date_trunc(ed.date, QUARTER) 
            when 'YEAR' then date_trunc(ed.date, YEAR)
        end as date
        , count(distinct ed.date) total_workouts
        , sum(ed.reps) total_reps
        , sum(ed.sets) total_sets
        , sum(ed.volume) total_volume
        , sum(ed.volume_load_lbs) total_volume_load_lbs
        , count(case when ed.exercise_label = 'Treadmill' then 1 end) total_runs
        , sum(case when ed.exercise_label = 'Treadmill' then ed.duration_min end) minutes_run
        , sum(case when ed.exercise_label = 'Treadmill' then ed.distance_mi end) miles_run
        , sum(case when ed.exercise_label = 'Treadmill' then ed.calories end) calories_burned
    from
        {{ref('cln_exercise_log')}} as ed
    cross join params as p
    where
        1=1
    group by
        1
)

, finance_data as (
    select
        case p.grain 
            when 'DAY' then date_trunc(fd.date, DAY) 
            when 'WEEK' then date_trunc(fd.date, WEEK) 
            when 'MONTH' then date_trunc(fd.date, MONTH)
            when 'QUARTER' then date_trunc(fd.date, QUARTER)  
            when 'YEAR' then date_trunc(fd.date, YEAR)
        end as date
        , abs(sum(case when fd.transaction_type = 'Revenue' then fd.total_amount end)) total_revenue
        , abs(sum(case when fd.transaction_type like '%Tax%' then fd.total_amount end)) total_tax
        , abs(sum(case when fd.transaction_type = 'Expense' then fd.total_amount end)) total_expenses
        , abs(sum(case when fd.transaction_type = 'Expense' and coalesce(fd.merchant, '') = '' then fd.total_amount end)) uncategorized_expenses
        , abs(sum(case when fd.transaction_type = 'Expense' and coalesce(fd.merchant, '') != '' then fd.total_amount end)) categorized_expenses
    from
        {{ref('rpt_finance')}} as fd
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
        , count(*) total_dishes
        , count(case when rd.dish_status = 'New' then 1 end) new_dishes
        , count(case when rd.dish_status = 'New' then 1 end) unique_dishes
        , sum(rd.cost) recipe_cost
    from
        {{ref('cln_recipe_log')}} as rd
    cross join params as p
    where
        1=1
    group by
        1 
)

, recipe_data_plants as (
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

, shopping_data as (
    select
        case p.grain 
            when 'DAY' then date_trunc(sd.date, DAY) 
            when 'WEEK' then date_trunc(sd.date, WEEK) 
            when 'MONTH' then date_trunc(sd.date, MONTH)
            when 'QUARTER' then date_trunc(sd.date, QUARTER) 
            when 'YEAR' then date_trunc(sd.date, YEAR)
        end as date
        , count(*) total_items_purchased
        , sum(sd.quantity) total_quantity_purchased
        , sum(sd.price) total_spend
    from
        {{ref('cln_shopping_log')}} as sd
    cross join params as p
    where
        1=1
    group by
        1 
)

, weight_data as (
    select
        case p.grain 
            when 'DAY' then date_trunc(wd.date, DAY) 
            when 'WEEK' then date_trunc(wd.date, WEEK) 
            when 'MONTH' then date_trunc(wd.date, MONTH)
            when 'QUARTER' then date_trunc(wd.date, QUARTER)  
            when 'YEAR' then date_trunc(wd.date, YEAR)
        end as date
        , count(*) total_measurements
        , avg(wd.weight) avg_weight
        , avg(wd.lean_body_mass) avg_lean_body_mass
        , avg(wd.bmi) avg_bmi
    from
        {{ref('cln_weights')}} as wd
    cross join params as p
    where
        1=1
    group by
        1
)

select
    dv.date
    
    -- Exercise data
    , ed.total_workouts
    , ed.total_reps
    , ed.total_sets
    , ed.total_volume
    , ed.total_volume_load_lbs
    , ed.total_runs
    , ed.minutes_run
    , ed.miles_run
    , ed.calories_burned

    -- Finance Data
    , fd.total_revenue
    , fd.total_tax
    , fd.total_expenses
    , fd.categorized_expenses
    , fd.uncategorized_expenses

    -- Recipe Data
    , rd.total_dishes
    , rd.recipe_cost

    -- Recipe Data cont. (flattened by plant)
    , rdp.unique_plants

    -- Shopping Data
    , sd.total_items_purchased
    , sd.total_quantity_purchased
    , sd.total_spend
    
    -- Weight Data
    , wd.total_measurements
    , wd.avg_weight
    , wd.avg_lean_body_mass
    , wd.avg_bmi

    -- Checks
    /* Obsolete: previously used to ensure extra data was not being added
    -- Attribute values could be useful but have no analytical value at present

    , coalesce(ed.total_workouts, 0)
    + coalesce(rd.total_dishes, 0)
    + coalesce(sd.total_items_purchased, 0)
    + coalesce(wd.total_measurements, 0) total_activity
    , sum(coalesce(ed.total_workouts, 0)
    + coalesce(rd.total_dishes, 0)
    + coalesce(sd.total_items_purchased, 0)
    + coalesce(wd.total_measurements, 0)) over (order by dv.date rows unbounded preceding) cumulative_activity

    */
from
    date_vector dv
left join exercise_data ed on
    dv.date = ed.date
left join finance_data fd on
    dv.date = fd.date
left join recipe_data rd on
    dv.date = rd.date
left join recipe_data_plants rdp on
    dv.date = rdp.date
left join shopping_data sd on
    dv.date = sd.date
left join weight_data wd on
    dv.date = wd.date