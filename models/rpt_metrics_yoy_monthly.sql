-- rpt_metrics_yoy_monthly.sql
-- Monthly trend (month grain) across every personal metric domain, from
-- 2025-01 forward: workouts / health (weights) / finance / spending /
-- nutrition (recipes) / LLM usage / coding activity (commits + line stats,
-- including the PrepSavvy and BibleTimes venture slices).
-- Built as the canonical source for YTD-vs-prior-year and YoY reporting.
{{ config(materialized='table', schema='dwh_reporting') }}

with months as (
    select date_trunc(d, month) as month
    from unnest(generate_date_array(
        date '2025-01-01',
        date_trunc(current_date(), month),
        interval 1 month)) as d
)

-- Life + finance + nutrition + shopping + weights: reuse the existing
-- monthly rollup (rpt_metrics_monthly) which already aggregates rpt_metrics.
, life_finance as (
    select
        date as month
        , total_workouts, total_reps, total_sets, total_volume
        , total_volume_load_lbs, total_runs, minutes_run, miles_run
        , calories_burned, unique_exercises
        , total_revenue, total_tax, total_expenses
        , categorized_expenses, uncategorized_expenses
        , unique_dishes, unique_plants, total_dishes, new_dishes
        , repeat_dishes, recipe_cost
        , total_items_purchased, total_quantity_purchased, total_spend
        , total_measurements, avg_weight, avg_lean_body_mass, avg_bmi
    from {{ ref('rpt_metrics_monthly') }}
    where date >= date '2025-01-01'
)

-- Coding activity, Mark's commits across both accounts, with venture slices.
, github as (
    select
        date_trunc(committed_date, month) as month
        , count(*)                         as commits
        , sum(additions)                   as lines_added
        , sum(deletions)                    as lines_deleted
        , sum(net_lines)                    as net_lines
        , sum(files_changed)                as files_changed
        , countif(venture = 'PrepSavvy')   as ps_commits
        , sum(if(venture = 'PrepSavvy', net_lines, 0)) as ps_net_lines
        , countif(venture = 'BibleTimes')  as bt_commits
        , sum(if(venture = 'BibleTimes', net_lines, 0)) as bt_net_lines
        , countif(venture = 'Personal')    as personal_commits
        , sum(if(venture = 'Personal', net_lines, 0)) as personal_net_lines
    from {{ ref('cln_github_activity') }}
    where is_mark = true
    group by 1
)

-- LLM spend + tokens + requests (all vendors).
, llm as (
    select
        d as month
        , sum(cost)          as llm_cost
        , sum(total_tokens)  as llm_tokens
        , sum(requests)      as llm_requests
    from `data-warehouse-475122.data_finance_business.fct_llm_usage_daily`
    group by 1
)

select
    m.month

    -- Workouts / muscle activity
    , lf.total_workouts
    , lf.total_reps
    , lf.total_sets
    , lf.total_volume
    , lf.total_volume_load_lbs
    , lf.total_runs
    , lf.minutes_run
    , lf.miles_run
    , lf.calories_burned
    , lf.unique_exercises

    -- Health (body composition)
    , lf.total_measurements
    , lf.avg_weight
    , lf.avg_lean_body_mass
    , lf.avg_bmi

    -- Finance
    , lf.total_revenue
    , lf.total_expenses
    , lf.categorized_expenses
    , lf.uncategorized_expenses

    -- Spending / nutrition
    , lf.total_spend
    , lf.total_items_purchased
    , lf.recipe_cost
    , lf.unique_dishes
    , lf.unique_plants

    -- Coding activity (total + per venture)
    , g.commits
    , g.lines_added
    , g.lines_deleted
    , g.net_lines
    , g.files_changed
    , g.ps_commits
    , g.ps_net_lines
    , g.bt_commits
    , g.bt_net_lines
    , g.personal_commits
    , g.personal_net_lines

    -- LLM
    , l.llm_cost
    , l.llm_tokens
    , l.llm_requests
from months m
left join life_finance lf on m.month = lf.month
left join github g        on m.month = g.month
left join llm l           on m.month = l.month
order by m.month
