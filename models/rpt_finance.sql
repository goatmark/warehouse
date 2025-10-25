-- rpt_finance.sql

with src as (
    select
        t.transaction_key

        -- dimensions
        , t.date
        , t.merchant
        , t.transaction_type
        , t.category
        , t.subcategory
        , t.description
        , t.is_interaccount
        , t.card_last4
        , t.amount

        -- intermediate columns
        , t.type_old
        , t.merchant_key_raw
        , t.merchant_key_assigned
        , t.category_raw
        , t.type_raw
        , t.description_lower
        , t.description_clean
        , t.intermediate_key
        , t.counter

        -- foreign keys
        , t.merchant_key
        , t.category_id
        , t.subcategory_id
    from
        {{ref('cln_card_transactions')}} as t

)

select
    src.date
    , src.transaction_type
    , coalesce(src.category, 'Unknown') category
    , coalesce(src.subcategory, 'Unknown') subcategory
    , coalesce(src.merchant, 'Unknown') merchant
    , coalesce(src.description, 'Unknown') description
    , coalesce(src.transaction_key, 'none') transaction_key
    , sum(src.amount) total_amount
from
    src
where
    1=1
group by
    1
    , 2
    , 3
    , 4
    , 5
    , 6
    , 7