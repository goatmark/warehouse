-- rpt_metrics.sql
{{ config(materialized='view') }}

with date_vector as (
    select
        *
    from
        unnest(generate_date_array(
        date_trunc(date '2021-10-25', day), -- Earliest measurement 
        date_trunc(current_date(), day),    -- Current date
        interval 1 day
      )) as date
)

, exercise_data as (
    select  
        ed.date as date
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
    where
        1=1
    group by
        1
)

, finance_data as (
    select
        fd.date as date
        , abs(sum(case when fd.transaction_type = 'Revenue' then fd.total_amount end)) total_revenue
        , abs(sum(case when fd.transaction_type like '%Tax%' then fd.total_amount end)) total_tax
        , abs(sum(case when fd.transaction_type = 'Expense' then fd.total_amount end)) total_expenses
        , abs(sum(case when fd.transaction_type = 'Expense' and coalesce(fd.merchant, '') = '' then fd.total_amount end)) uncategorized_expenses
        , abs(sum(case when fd.transaction_type = 'Expense' and coalesce(fd.merchant, '') != '' then fd.total_amount end)) categorized_expenses
    from
        {{ref('rpt_finance')}} as fd
    where
        1=1
    group by
        1
)

, recipe_data as (
    select
        rd.date as date
        , count(*) total_dishes
        , count(case when rd.dish_status = 'New' then 1 end) new_dishes
        , count(case when rd.dish_status = 'Repeat' then 1 end) repeat_dishes
        , sum(rd.cost) recipe_cost
    from
        {{ref('cln_recipe_log')}} as rd
    where
        1=1
    group by
        1 
)

, shopping_data as (
    select
        sd.date as date
        , count(*) total_items_purchased
        , sum(sd.quantity) total_quantity_purchased
        , sum(sd.price) total_spend
    from
        {{ref('cln_shopping_log')}} as sd
    where
        1=1
    group by
        1 
)

, weight_data as (
    select
        wd.date as date
        , count(*) total_measurements
        , avg(wd.weight) avg_weight
        , avg(wd.lean_body_mass) avg_lean_body_mass
        , avg(wd.bmi) avg_bmi
    from
        {{ref('cln_weights')}} as wd
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
    , rd.new_dishes
    , rd.repeat_dishes
    , rd.recipe_cost

    -- Shopping Data
    , sd.total_items_purchased
    , sd.total_quantity_purchased
    , sd.total_spend
    
    -- Weight Data
    , wd.total_measurements
    , wd.avg_weight
    , wd.avg_lean_body_mass
    , wd.avg_bmi
from
    date_vector dv
left join exercise_data ed on
    dv.date = ed.date
left join finance_data fd on
    dv.date = fd.date
left join recipe_data rd on
    dv.date = rd.date
left join shopping_data sd on
    dv.date = sd.date
left join weight_data wd on
    dv.date = wd.date