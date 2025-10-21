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
        *
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

select
    *
from
    classified_transactions