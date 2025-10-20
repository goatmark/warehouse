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

select
    *
from
    src_merchants