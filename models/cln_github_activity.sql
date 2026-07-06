-- cln_github_activity.sql
-- Cleaned per-commit coding activity from the data_github raw source.
-- Adds line stats (additions/deletions/files) that the legacy
-- cln_github_commits model does not have, plus date grains and an
-- owner flag for filtering reports to Mark's own commits.
{{ config(materialized='view') }}

with src as (
    select
        sha
        , repo
        , split(repo, '/')[safe_offset(1)]              as repo_name
        , split(message, '\n')[safe_offset(0)]          as message
        , author_name
        , author_email
        , author_login
        , authored_at
        , committed_at
        , cast(date_trunc(committed_at, day) as date)   as committed_date
        , cast(date_trunc(committed_at, week) as date)  as committed_week
        , cast(date_trunc(committed_at, month) as date) as committed_month
        , coalesce(additions, 0)                        as additions
        , coalesce(deletions, 0)                        as deletions
        , coalesce(additions, 0) - coalesce(deletions, 0) as net_lines
        , coalesce(files_changed, 0)                    as files_changed
        , (author_login = 'goatmark')                   as is_owner
        , url
    from
        {{ source('data_github', 'commits') }}
    where
        committed_at is not null
)

select * from src
