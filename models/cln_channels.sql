-- cln_channels.sql
{{ config(materialized='view')}}

with src as (
    select
        cast(src.id as string)                  as id_raw
        , cast(src.handle as string)            as handle
        , cast(src.channel_id as string)        as id
        , cast(src.created_at as timestamp)     as created_at
        , cast(src.updated_at as timestamp)     as updated_at
    from
        {{source('warehouse', 'channels')}} as src
)

select
    id
    , handle
    , created_at
    , updated_at
from
    src