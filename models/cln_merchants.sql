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
        {{source('finance_personal', 'merchants')}}
)

select
    m.id
    , m.merchant_name
    , m.merchant_key
    , s.name subcategory_name
    , c.name category_name
    , c.id category_id
    , s.id subcategory_id
from
    src_merchants m
left join {{source('finance_personal', 'subcategories')}} as s on
    s.id = m.subcategory_id
left join {{source('finance_personal', 'categories')}} as c on
    c.id = s.category_id