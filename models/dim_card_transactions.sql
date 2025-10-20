with src as (
    select
        *
    from
        {{ref('cln_card_transactions')}}
    where
        1=1
)

, merchants as (
    select
        *
    from
        warehouse.merchants
)

, classified_transactions as (
    select
        *
    from
        src
)

select
    *
from
    classified_transactions