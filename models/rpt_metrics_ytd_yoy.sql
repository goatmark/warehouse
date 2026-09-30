-- rpt_metrics_ytd_yoy.sql
-- Year-to-date vs prior-year YTD (same Jan-to-current-month window),
-- one row per metric with unit, ytd_current, ytd_prior, abs and pct deltas.
-- Additive metrics are summed; body-composition metrics (weight / LBM / BMI)
-- are measurement-weighted averages across the window.
{{ config(materialized='view', schema='dwh_reporting') }}

with m as (
    select * from {{ ref('rpt_metrics_yoy_monthly') }}
)

-- Current-year YTD window: Jan 1 .. end of current month.
, cur as (
    select * from m
    where extract(year from month) = extract(year from current_date())
      and month <= date_trunc(current_date(), month)
)

-- Prior-year same window: Jan 1 .. end of the same month one year earlier.
, prior as (
    select * from m
    where extract(year from month) = extract(year from current_date()) - 1
      and month <= date_add(date_trunc(current_date(), month), interval -1 year)
)

-- Single-row additive sums per window.
, cur_sum as (
    select
        sum(total_workouts)         as workouts
        , sum(total_reps)            as reps
        , sum(total_sets)            as sets
        , sum(total_volume)          as volume
        , sum(total_volume_load_lbs) as volume_load_lbs
        , sum(total_runs)            as runs
        , sum(minutes_run)           as run_minutes
        , sum(miles_run)             as run_miles
        , sum(calories_burned)       as calories_burned
        , sum(total_revenue)         as revenue
        , sum(total_expenses)        as expenses
        , sum(categorized_expenses)  as categorized_spend
        , sum(uncategorized_expenses) as uncategorized_spend
        , sum(total_spend)           as shopping_spend
        , sum(recipe_cost)           as recipe_cost
        , sum(commits)               as commits
        , sum(lines_added)           as lines_added
        , sum(lines_deleted)          as lines_deleted
        , sum(net_lines)             as net_lines
        , sum(files_changed)         as files_changed
        , sum(ps_commits)            as ps_commits
        , sum(ps_net_lines)          as ps_net_lines
        , sum(bt_commits)            as bt_commits
        , sum(bt_net_lines)          as bt_net_lines
        , sum(llm_cost)              as llm_spend
        , sum(llm_tokens)            as llm_tokens
        , sum(llm_requests)          as llm_requests
        , sum(avg_weight * total_measurements)         / nullif(sum(total_measurements), 0) as avg_weight
        , sum(avg_lean_body_mass * total_measurements) / nullif(sum(total_measurements), 0) as avg_lbm
        , sum(avg_bmi * total_measurements)            / nullif(sum(total_measurements), 0) as avg_bmi
    from cur
)

, prior_sum as (
    select
        sum(total_workouts)         as workouts
        , sum(total_reps)            as reps
        , sum(total_sets)            as sets
        , sum(total_volume)          as volume
        , sum(total_volume_load_lbs) as volume_load_lbs
        , sum(total_runs)            as runs
        , sum(minutes_run)           as run_minutes
        , sum(miles_run)             as run_miles
        , sum(calories_burned)       as calories_burned
        , sum(total_revenue)         as revenue
        , sum(total_expenses)        as expenses
        , sum(categorized_expenses)  as categorized_spend
        , sum(uncategorized_expenses) as uncategorized_spend
        , sum(total_spend)           as shopping_spend
        , sum(recipe_cost)           as recipe_cost
        , sum(commits)               as commits
        , sum(lines_added)           as lines_added
        , sum(lines_deleted)          as lines_deleted
        , sum(net_lines)             as net_lines
        , sum(files_changed)         as files_changed
        , sum(ps_commits)            as ps_commits
        , sum(ps_net_lines)          as ps_net_lines
        , sum(bt_commits)            as bt_commits
        , sum(bt_net_lines)          as bt_net_lines
        , sum(llm_cost)              as llm_spend
        , sum(llm_tokens)            as llm_tokens
        , sum(llm_requests)          as llm_requests
        , sum(avg_weight * total_measurements)         / nullif(sum(total_measurements), 0) as avg_weight
        , sum(avg_lean_body_mass * total_measurements) / nullif(sum(total_measurements), 0) as avg_lbm
        , sum(avg_bmi * total_measurements)            / nullif(sum(total_measurements), 0) as avg_bmi
    from prior
)

, both as (
    select c.workouts  as cur, p.workouts  as pri, 'Workouts'        metric, 'count'  unit from cur_sum c cross join prior_sum p
    union all select c.reps,           p.reps,           'Reps',            'reps'    from cur_sum c cross join prior_sum p
    union all select c.sets,            p.sets,            'Sets',            'sets'    from cur_sum c cross join prior_sum p
    union all select c.volume,          p.volume,          'Volume',          'volume'  from cur_sum c cross join prior_sum p
    union all select c.volume_load_lbs, p.volume_load_lbs, 'Volume load',     'lbs'     from cur_sum c cross join prior_sum p
    union all select c.runs,            p.runs,            'Runs',            'count'   from cur_sum c cross join prior_sum p
    union all select c.run_minutes,    p.run_minutes,     'Run minutes',     'min'     from cur_sum c cross join prior_sum p
    union all select c.run_miles,      p.run_miles,       'Run miles',       'mi'      from cur_sum c cross join prior_sum p
    union all select c.calories_burned,p.calories_burned, 'Calories burned', 'kcal'    from cur_sum c cross join prior_sum p
    union all select c.revenue,        p.revenue,         'Revenue',         'USD'     from cur_sum c cross join prior_sum p
    union all select c.expenses,       p.expenses,        'Expenses',        'USD'     from cur_sum c cross join prior_sum p
    union all select c.categorized_spend,  p.categorized_spend,  'Categorized spend',   'USD' from cur_sum c cross join prior_sum p
    union all select c.uncategorized_spend,p.uncategorized_spend,'Uncategorized spend','USD' from cur_sum c cross join prior_sum p
    union all select c.shopping_spend, p.shopping_spend,  'Shopping spend',  'USD'     from cur_sum c cross join prior_sum p
    union all select c.recipe_cost,    p.recipe_cost,     'Recipe cost',     'USD'     from cur_sum c cross join prior_sum p
    union all select c.commits,        p.commits,         'Commits',         'count'   from cur_sum c cross join prior_sum p
    union all select c.lines_added,    p.lines_added,     'Lines added',     'lines'   from cur_sum c cross join prior_sum p
    union all select c.lines_deleted, p.lines_deleted,   'Lines deleted',   'lines'   from cur_sum c cross join prior_sum p
    union all select c.net_lines,      p.net_lines,       'Net lines',       'lines'   from cur_sum c cross join prior_sum p
    union all select c.files_changed, p.files_changed,    'Files changed',   'files'   from cur_sum c cross join prior_sum p
    union all select c.ps_commits,    p.ps_commits,       'PrepSavvy commits','count'  from cur_sum c cross join prior_sum p
    union all select c.ps_net_lines, p.ps_net_lines,      'PrepSavvy net lines','lines' from cur_sum c cross join prior_sum p
    union all select c.bt_commits,    p.bt_commits,       'BibleTimes commits','count' from cur_sum c cross join prior_sum p
    union all select c.bt_net_lines, p.bt_net_lines,      'BibleTimes net lines','lines' from cur_sum c cross join prior_sum p
    union all select c.llm_spend,    p.llm_spend,         'LLM spend',       'USD'     from cur_sum c cross join prior_sum p
    union all select c.llm_tokens,   p.llm_tokens,        'LLM tokens',      'tokens'  from cur_sum c cross join prior_sum p
    union all select c.llm_requests, p.llm_requests,     'LLM requests',    'count'   from cur_sum c cross join prior_sum p
    union all select c.avg_weight,   p.avg_weight,        'Avg weight',      'lbs'     from cur_sum c cross join prior_sum p
    union all select c.avg_lbm,      p.avg_lbm,           'Avg lean body mass','lbs'   from cur_sum c cross join prior_sum p
    union all select c.avg_bmi,       p.avg_bmi,          'Avg BMI',         'index'   from cur_sum c cross join prior_sum p
)

select
    metric
    , unit
    , round(cur, 2) as ytd_2026
    , round(pri, 2) as ytd_2025
    , round(cur - pri, 2) as yoy_abs
    , case when pri is null or pri = 0 then null
           else round((cur - pri) / pri * 100, 1) end as yoy_pct
from both
order by metric
