-- rpt_finance_monthly.sql

with src as (
    select
        src.date
        , src.transaction_type
        , src.category
        , src.subcategory
        , src.merchant
        , src.total_amount
    from
        {{ref('rpt_finance')}} as src

)

select
    date_trunc(src.date, month) date
    , src.transaction_type
    , src.category
    , src.subcategory
    , src.merchant
    , sum(src.total_amount) total_amount
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