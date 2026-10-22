{{ config(materialized='table', schema='data_finance_business') }}

-- Diagnostic operational costs only. They are never UNIONed into financial facts.
SELECT * EXCEPT(loaded_at) FROM {{ source('infra','infra_operational_usage_events') }}
QUALIFY ROW_NUMBER() OVER(PARTITION BY record_id ORDER BY loaded_at DESC)=1
