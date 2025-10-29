-- cln_github_commits.sql
{{ config(materialized='view') }}

with src as (
    select
        cast(url as string)                             as commit_url
        , cast(comments_url as string)                  as comments_url
        , cast(html_url as string)                      as html_url
        , cast(author as json)                          as author
        , cast(branch as string)                        as branch
        , cast(commit as json)                          as commit
        , cast(node_id as string)                       as node_id
        , cast(parents as json)                         as parents
        , cast(committer as json)                       as committer
        , cast(created_at as timestamp)                 as created_at
        , cast(date_trunc(created_at, day) as date)     as date
        , cast(repository as string)                    as repository_full
        , substr(cast(repository as string), 
            strpos(cast(repository as string), 
            '/') + 1)                                   as repository
    from
        {{source('github', 'commits')}}
)

select
    src.date
    , src.commit_url as commit_url
    , src.repository as repository
from
    src
where
    1=1