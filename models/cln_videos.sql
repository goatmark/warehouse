-- cln_channels.sql
{{ config(materialized='view')}}

with src as (
    select
        cast(src.id as string)                  as id_raw
        , cast(src.handle as string)            as handle
        , cast(src.channel_id as string)        as channel_id
        , cast(src.created_at as timestamp)     as created_at
        , cast(src.updated_at as timestamp)     as updated_at
    from
        {{source('warehouse', 'channels')}} as src
)

, youtube_comments_data as (
    select
        cast(videoId as string)             as video_id
        , cast(channelId as string)         as channel_id
        , cast(canReply as boolean)         as can_reply
        , cast(isPublic as boolean)         as is_public
        , cast(topLevelComment as json)     as comment_json
        , cast(totalReplyCount as int64)    as total_replies
        , json_extract_scalar(topLevelComment, '$.etag') as comment_id
        , json_extract_scalar(topLevelComment, '$.snippet.textOriginal') as text_original
        , regexp_extract(
            json_extract_scalar(topLevelComment, '$.snippet.authorChannelUrl'),
            r'/([^/]+)$',
            1
        ) as author_channel_id
    from
        {{source('youtube', 'comments')}} as c
    where
        1=1
)

, comments_agg as (
    select
        yt_c.channel_id                                         as channel_id
        , count(yt_c.comment_id)                                as total_comments
        , count(case when yt_c.total_replies > 0 then 1 end)    as comments_with_replies
        , sum(yt_c.total_replies)                               as total_replies
        , count(distinct yt_c.video_id)                         as videos_with_comments
    from
        youtube_comments_data as yt_c
    group by
        1
)

, youtube_video_data as (
    select
        cast(videoId as string) as video_id
        , cast(title as string) as title
        , cast(datetime as timestamp) as uploaded_at
        , cast(channelId as string) as channel_id
        , cast(localized as json) as localized_json
        , cast(description as string) as description
        , cast(publishedAt as timestamp) as published_at
        , cast(categoryId as int64) as category_id
        , cast(channelTitle as string) as channel_title
        , cast(defaultLanguage as string) as default_language_code
        , json_extract_scalar(thumbnails, '$.default.url') as thumbnail_default_url
        , json_extract_scalar(thumbnails, '$.medium.url') as thumbnail_medium_url
        , json_extract_scalar(thumbnails, '$.high.url') as thumbnail_high_url
        , cast(c.name as string) as category 
    from
        {{source('youtube', 'video')}} as v
    left join {{source('warehouse', 'youtube_categories')}} as c on
        cast(c.id as int64) = cast(categoryId as int64)
    where
        1=1
) 

, videos_agg as (
    select
        channel_id
        , count(*) total_videos
    from
        youtube_video_data
    where
        1=1
    group by
        1
)

select
    src.channel_id
    , src.handle
    , src.id_raw 
    , src.created_at
    , src.updated_at

    -- Comments Data
    , c.total_comments
    , c.comments_with_replies
    , c.total_replies
    , c.videos_with_comments
    
    -- Videos Data
    , v.total_videos

     -- Channel Ratios
    , c.comments_with_replies / nullif(c.total_replies, 0) comment_reply_rate
    , c.total_replies / nullif(c.comments_with_replies, 0) replies_per_comment
    , c.videos_with_comments / nullif(v.total_videos, 0) videos_with_replies_rate
from
    src
left join comments_agg c on
    src.channel_id = c.channel_id
left join videos_agg v on
    src.channel_id = v.channel_id