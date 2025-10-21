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
        , cast(category as string)              as category_raw
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
        s.key
        , s.description
        , s.date
        , s.type_raw
        , s.category_raw
        , s.amount
        , s.card_last4
        , s.counter
        , s.intermediate_key
        , s.description_lower
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
        , m.merchant_key        as merchant_key
        , m.merchant_name       as merchant_name
        , m.subcategory_id      as subcategory_id
        , m.subcategory_name    as subcategory_name
        , m.category_id         as category_id 
        , m.category_name       as category_name
    from
        classified_transactions ct
    left join {{source('warehouse', 'transaction_merchant_maps')}} as map on
        map.transaction_key = ct.key
    left join {{ref('cln_merchants')}} as m on
        m.merchant_key = map.merchant_key
)

select
    t.key
    , t.date
    , t.merchant_name
    , t.subcategory_name
    , t.category_name
    , t.amount
    , t.description
    , t.type
    , t.card_last4
    , t.intermediate_key
    , t.counter
    , t.description_lower

    , t.category_raw
    , t.type_raw
    , t.merchant_key
    , t.subcategory_id
    , t.category_id
from
    matched_transactions t