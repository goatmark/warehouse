-- tmp_unclassified_transactions.sql
{{ config(materialized='view') }}

select
    t.date
    , t.description
    , t.amount
    , t.card_last4
    , t.is_interaccount 
from
    {{ref('cln_card_transactions')}} as t
where
    1=1
    and t.transaction_type = 'Expense'
    and (coalesce(t.merchant, '') = '' or coalesce(t.category, '') = '')
order by
    t.date desc
    , t.amount asc