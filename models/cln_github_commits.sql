-- cln_github_commits.sql
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
        , committed_at
        , cast(date_trunc(committed_at, day) as date)   as committed_date
        , cast(date_trunc(committed_at, week) as date)  as committed_week
        , cast(date_trunc(committed_at, month) as date) as committed_month
        , url
    from
        {{ source('github', 'commits_clean') }}
    where
        committed_at is not null
)

select * from src
