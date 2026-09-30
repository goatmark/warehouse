-- cln_github_activity.sql
-- Cleaned per-commit coding activity from the data_github raw source.
-- Adds line stats (additions/deletions/files) that the legacy
-- cln_github_commits model does not have, plus date grains, a venture
-- dimension (BibleTimes / PrepSavvy / Personal / Other) derived from the
-- repo, and an is_mark flag covering BOTH Mark-owned GitHub accounts
-- (goatmark + prepsavvy) and his author emails, so reports count Mark's
-- work across every venture.
{{ config(materialized='view') }}

with src as (
    select
        sha
        , repo
        , split(repo, '/')[safe_offset(0)]              as repo_owner
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
        , url
    from
        {{ source('data_github', 'commits') }}
    where
        committed_at is not null
)

, tagged as (
    select
        s.*
        -- Venture dimension. PrepSavvy is keyed off the repo owner
        -- (separate GitHub account); BibleTimes / Personal / Other are
        -- keyed off the goatmark repo name.
        , case
            when s.repo_owner = 'prepsavvy' or s.repo_name like 'prepsavvy%'
                then 'PrepSavvy'
            when s.repo_name like 'bibletimes%'
                  or s.repo_name in ('index-pusher', 'pageblitz', 'mysite-church')
                then 'BibleTimes'
            when s.repo_name in (
                  'website', 'telos-brand', 'star-data'
                , 'jarvis', 'jarvis-command-center', 'warehouse', 'ledger'
                , 'kalshi-listener', 'equity-research', 'neuro-sim'
                , 'marsh-intelligence', 'tails-intelligence'
                , 'creator-search-intelligence', 'beauty-intelligence'
                , 'zurich-hunt', 'grokbots'
            )
                then 'Personal'
            else 'Other'
          end                                              as venture
        -- Mark's own commits across both accounts + his known author emails.
        , (
            s.author_login in ('goatmark', 'prepsavvy')
            or s.author_email in (
                'mark@markkhoury.me', 'm.majdalani.khoury@gmail.com'
            )
          )                                               as is_mark
    from src s
)

select * from tagged
