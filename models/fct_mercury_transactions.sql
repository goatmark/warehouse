{{ config(materialized='table') }}

with src as (
    select * from {{ source('finance', 'mercury_transactions') }}
    where status in ('sent', 'pending')
      and posted_at is not null
      and (
        -- Credit card account: exclude only internal account-to-account payment credits
        (account_id = '1eeebdc0-7891-11f0-b269-471e5e3e0a24'
         and not regexp_contains(lower(coalesce(counterparty_name, '')), r'mercury checking|mercury savings'))
        -- Plus cashback/interest from any account
        or regexp_contains(lower(coalesce(counterparty_name, '')), r'mercury io cashback')
      )
)

select
    transaction_id,
    amount,
    timestamp(posted_at)                          as posted_at,
    date(timestamp(posted_at))                    as posted_date,
    format_date('%Y-%m', date(timestamp(posted_at))) as year_month,
    extract(year  from timestamp(posted_at))      as year,
    extract(month from timestamp(posted_at))      as month,
    timestamp(created_at)                         as created_at,
    status,
    case when amount < 0 then 'expense' else 'income' end as direction,

    -- Vendor normalisation
    case
        when regexp_contains(lower(counterparty_name), r'anthropic')                          then 'Anthropic'
        when regexp_contains(lower(counterparty_name), r'openai|chatgpt')                    then 'OpenAI'
        when regexp_contains(lower(counterparty_name), r'twitter|x corp')                    then 'X/Twitter'
        when regexp_contains(lower(counterparty_name), r'github')                             then 'GitHub'
        when regexp_contains(lower(counterparty_name), r'\bxai\b|x\.ai')                     then 'xAI'
        when regexp_contains(lower(counterparty_name), r'\bn8n\b')                            then 'n8n'
        when regexp_contains(lower(counterparty_name), r'supabase')                           then 'Supabase'
        when regexp_contains(lower(counterparty_name), r'google')                             then 'Google Workspace'
        when regexp_contains(lower(counterparty_name), r'digitalocean|digital ocean')        then 'DigitalOcean'
        when regexp_contains(lower(counterparty_name), r'vercel')                             then 'Vercel'
        when regexp_contains(lower(counterparty_name), r'british airways|ba inflight')        then 'British Airways'
        else counterparty_name
    end as vendor,

    -- Expense category (mirrors the Finances tab structure)
    case
        when amount > 0 and regexp_contains(lower(mercury_category), r'interest')            then 'Interest Earned'
        when amount > 0 and regexp_contains(lower(coalesce(counterparty_name, '')), r'mercury io cashback') then 'Cashback & Rewards'
        when amount > 0                                                                       then 'Other Income'
        when regexp_contains(lower(counterparty_name), r'anthropic|openai|github|supabase|n8n|google|xai|x\.ai|vercel|digitalocean')
                                                                                              then 'Software & Subscriptions'
        when regexp_contains(lower(mercury_category), r'software|subscription')              then 'Software & Subscriptions'
        when regexp_contains(lower(counterparty_name), r'twitter|x corp|facebook|instagram|meta') then 'Marketing & Advertising'
        when regexp_contains(lower(mercury_category), r'marketing|advertising')              then 'Marketing & Advertising'
        when regexp_contains(lower(mercury_category), r'travel|transport|airlines|flight')   then 'Travel & Transportation'
        when regexp_contains(lower(counterparty_name), r'british airways|ba inflight|united|delta|american airlines') then 'Travel & Transportation'
        when regexp_contains(lower(mercury_category), r'restaurant|food|dining|meal')        then 'Business Meals'
        when regexp_contains(lower(mercury_category), r'office|equipment|supplies|hardware') then 'Office Supplies & Equipment'
        else coalesce(mercury_category, 'Other')
    end as expense_category,

    mercury_category,
    merchant_category,
    bank_description,
    note,
    dashboard_link,
    has_receipt,
    has_generated_receipt,
    account_id,
    loaded_at

from src
