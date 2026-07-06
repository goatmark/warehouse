{{ config(materialized='table') }}

-- Monthly P&L by category. Long format — pivot in Sheets if columns-by-month needed.
-- Income rows have positive amount. Expense rows have negative amount.

select
    year_month,
    year,
    month,
    direction,
    expense_category,
    sum(amount)        as total_amount,
    count(*)           as transaction_count
from {{ ref('fct_mercury_transactions') }}
-- Real P&L only recognizes settled, external money movement: exclude
-- interaccount transfers (self-moves, not income/expense) and anything
-- that never actually settled (failed/pending).
where not is_interaccount
  and status = 'sent'
group by 1, 2, 3, 4, 5
order by 1 desc, 4, 5
