-- fct_card_transactions.sql
{{ config(materialized='view') }}

-- Import files
with src_transactions as (
    select
        *
    from 
        {{ ref('cln_card_transactions') }}
    where 
        1=1
)

, src_merchants as (
    select 
        *
    from 
        {{ ref('cln_merchants') }}
)

select
    t.key
    , t.description
    , case
        when regexp_contains(t.description, 'AAA') then 'AAA'
        else 'Other'
    end merchant_match
    , t.date
    , t.amount
    , t.type
from
    src_transactions t