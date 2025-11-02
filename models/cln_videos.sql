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

, transcripts as (
    select
        vt.id
        , vt.video_id
        , vt.preview
        , vt.created_at
        , vt.updated_at
        , vt.word_count
        , vt.storage_path
    from
        {{source('warehouse', 'video_transcripts')}} as vt
    where
        1=1
        and vt.status = 'completed'
)

select
    src.id
    , src.title
    , src.description
    , src.thumbnail_url
    , src.has_transcript
    , src.view_count
    , src.url
    , t.preview as transcript_preview
    , t.word_count as transcript_word_count
    , t.storage_path as transcript_storage_path
    , src.channel_id
    , src.created_at
    , src.updated_at
from
    src
left join transcripts t on
    src.id = t.video_id