-- rpt_github_activity.sql
-- Coding-activity reporting model: repo x day grain, owner commits only.
-- Roll up downstream for weekly / monthly / per-repo totals and
-- most-active-repo reporting. Materialized into the dwh_reporting dataset.
{{ config(materialized='table', schema='dwh_reporting') }}

with commits as (
    select *
    from {{ ref('cln_github_activity') }}
    where is_owner = true
)

, daily as (
    select
        repo
        , repo_name
        , committed_date
        , committed_week
        , committed_month
        , count(*)              as commits
        , sum(additions)        as lines_added
        , sum(deletions)        as lines_deleted
        , sum(net_lines)        as net_lines
        , sum(files_changed)    as files_changed
    from
        commits
    group by
        1, 2, 3, 4, 5
)

select
    repo
    , repo_name
    , committed_date
    , committed_week
    , committed_month
    , commits
    , lines_added
    , lines_deleted
    , net_lines
    , files_changed
from
    daily
order by
    committed_date desc, commits desc
