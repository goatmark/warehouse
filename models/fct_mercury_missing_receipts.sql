{{ config(materialized='table') }}

select
    transaction_id,
    posted_date,
    year_month,
    amount,
    vendor,
    expense_category,
    bank_description,
    status,
    dashboard_link
from {{ ref('fct_mercury_transactions') }}
where amount < 0
  and not has_receipt
  and not has_generated_receipt
  and not regexp_contains(lower(vendor), r'intl\.? transaction fee|international transaction fee')
order by posted_date desc
