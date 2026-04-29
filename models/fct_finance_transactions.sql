{{ config(materialized='table') }}

-- Consolidated transaction ledger: Mercury card + personal card (Notion, Aug–Sep 2025).
-- Grain: one row per transaction. Use this as the single source of truth for P&L.

select
    transaction_id,
    posted_date,
    format_date('%Y-%m', posted_date)   as year_month,
    extract(year  from posted_date)     as year,
    extract(month from posted_date)     as month,
    amount,
    case when amount < 0 then 'expense' else 'income' end as direction,
    vendor,
    expense_category,
    'mercury'                           as source,
    has_receipt,
    dashboard_link

from {{ ref('fct_mercury_transactions') }}

union all

select
    transaction_id,
    posted_date,
    format_date('%Y-%m', posted_date)   as year_month,
    extract(year  from posted_date)     as year,
    extract(month from posted_date)     as month,
    amount,
    'expense'                           as direction,
    vendor,
    expense_category,
    source,
    true                                as has_receipt,
    cast(null as string)                as dashboard_link

from {{ source('finance', 'notion_transactions') }}
