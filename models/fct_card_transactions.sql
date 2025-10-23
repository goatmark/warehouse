-- fct_card_transactions.sql 
{{ config(materialized='view') }} 

/*

with src as (
    select
        src.transaction_key

        -- dimensions
        , src.date
        , src.merchant
        , src.type
        , src.category
        , src.subcategory
        , src.description
        , src.card_last4
        , src.amount

        /* -- Commenting out. Marginal analytical value
        -- intermediate columns
        , src.merchant_key_raw
        , src.merchant_key_assigned
        , src.category_raw
        , src.type_raw
        , src.description_lower
        , src.intermediate_key
        , src.counter
        */

        -- foreign keys
        , src.merchant_key
        , src.category_id
        , src.subcategory_id
    from
        {{ref('cln_card_transactions')}}
)

select
    *
from
    src
where
    1=1

*/

select 1