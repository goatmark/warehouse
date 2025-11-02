-- cln_videos.sql
{{ config(materialized='view')}}
 
with src as (
    select
        cast(src.id as string)                  as id_raw
        , cast(src.video_id as string)          as id
        , cast(src.title as string)             as title
        , cast(src.description as string)       as description
        , cast(src.thumbnail_url as string)     as thumbnail_url
        , cast(src.has_transcript as boolean)   as has_transcript
        , cast(src.view_count as int64)         as view_count
        , cast(src.url as string)               as url 
        , cast(src.channel_id as string)        as channel_id
        , cast(src.created_at as timestamp)     as created_at
        , cast(src.updated_at as timestamp)     as updated_at
    from
        {{source('warehouse', 'videos')}} as src
)

select
    id
    , title
    , description
    , thumbnail_url
    , has_transcript
    , view_count
    , url
    , channel_id
    , created_at
    , updated_at
from
    src