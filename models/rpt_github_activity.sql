-- rpt_github_activity.sql
-- Coding-activity reporting model: venture x repo x day grain, Mark's
-- commits only (both goatmark + prepsavvy accounts). Carries the venture
-- dimension so downstream rollups can break activity out by venture.
-- Materialized into the dwh_reporting dataset.
{{ config(materialized='table', schema='dwh_reporting') }}

with commits as (
    select *
    from {{ ref('cln_github_activity') }}
    where is_mark = true
)

, daily as (
    select
        venture
        , repo
        , repo_name
        , repo_owner
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
        1, 2, 3, 4, 5, 6, 7
)

select
    venture
    , repo
    , repo_name
    , repo_owner
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
