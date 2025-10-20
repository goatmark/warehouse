with src_transactions as (
    select
        *
    from
        {{ref('cln_card_transactions')}}
    where
        1=1
)

, src_merchants as (
    select
        *
    from
        {{ref('cln_merchants')}}
)

, merchant_regex as (
    select
        cast(regex_key as string)           as regex_key
        , cast(merchant_key as string)      as merchant_key
        , cast(pattern_regex as string)     as pattern_regex
    from
        warehouse.merchant_regex
)
select
    t.key transaction_key
    , t.description
    , t.description_lower
    , t.date
    , t.category
    , t.amount
    , t.card_last4
    , t.type
from
    src_transactions t