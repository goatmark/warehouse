-- cln_merchants.sql
{{ config(materialized='view') }}

-- Import file and cast
with src_merchants as (
    select
        cast(id as int64)                   as id
        , cast(name as string)              as merchant_name
        , cast(merchant_key as string)      as merchant_key
        , cast(subcategory_id as int64)     as subcategory_id
    from
        {{source('warehouse', 'merchants')}}
)

select
    *
from
    src_merchants