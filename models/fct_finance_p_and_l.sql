{{ config(materialized='table') }}

-- Monthly P&L by category — full company view (Mercury + Notion personal card).
-- Income rows have positive total_amount. Expense rows have negative total_amount.
-- Grain: one row per year_month + direction + expense_category.

select
    year_month,
    year,
    month,
    direction,
    expense_category,
    source,
    sum(amount)     as total_amount,
    count(*)        as transaction_count
from {{ ref('fct_finance_transactions') }}
group by 1, 2, 3, 4, 5, 6
order by 1 desc, 4, 5
