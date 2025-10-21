-- cln_card_transactions.sql
{{ config(materialized='view') }}

-- Import file and cast
    -- Note:  field `description_lower` added here to improve downstream query performance
with src as (
    select
        cast(key as string)                     as key
        , cast(description as string)           as description
        , cast(date as date)                    as date
        , cast(type as string)                  as type_raw
        , cast(category as string)              as category
        , safe_cast(amount as numeric)          as amount
        , safe_cast(card_last4 as int64)        as card_last4
        , safe_cast(counter as numeric)         as counter
        , safe_cast(intermediate_key as string) as intermediate_key
        , lower(cast(description as string))    as description_lower
    from
        {{source('warehouse', 'card_transactions')}}
    where
        1=1
)

, classified_transactions as (
    select
        key
        , description
        , date
        , type_raw
        , category
        , amount
        , card_last4
        , counter
        , intermediate_key
        , description_lower
        , case
            when s.type_raw is not null then s.type_raw
            when s.card_last4 not in ({{ var('payment_card_last4_list', [3221,4245,5083,6823]) | join(',') }})
                then 'Payment'
            when {{ payments_type_keyword_match('s.description') }} 
                then 'Payment'
            else 'Sale'
        end as type
    from
        src as s
)

, matched_transactions as (
    select
        ct.*
        , m.name as merchant_name
        , s.id as subcategory_id
        , s.name as subcategory_name
        , c.id as category_id 
        , c.name as category_name
    from
        classified_transactions ct
    left join {{source('warehouse', 'transactions_merchants_map')}} as map on
        map.transaction_key = ct.key
    left join {{source('warehouse', 'merchants')}} as m on
        m.merchant_key = map.merchant_key
    left join {{source('warehouse', 'subcategories')}} as s on
        s.id = m.subcategory_id
    left join {{source('warehouse', 'categories')}} as c on
        c.id = s.category_id

)

select
    *
from
    matched_transactions