{{ config(materialized='table', schema='dwh_reporting') }}

-- Cost bases are deliberately separate. Never SUM across cost_basis: payments,
-- consumed usage, provider bills and the accounting ledger describe different events.
WITH llm AS (
  SELECT DATE_TRUNC(d, MONTH) AS month,
    project, service, environment, vendor, model, cost_basis, attribution_method,
    SUM(cost_usd) AS cost_usd,
    SUM(prompt_tokens) AS prompt_tokens,
    SUM(completion_tokens) AS completion_tokens,
    SUM(total_tokens) AS total_tokens
  FROM {{ ref('fct_infrastructure_cost_daily') }}
  GROUP BY 1,2,3,4,5,6,7,8
), hosting AS (
  SELECT DATE_TRUNC(e.entry_date, MONTH) AS month,
    COALESCE(b.project,'unattributed') AS project,COALESCE(b.service,'unattributed') AS service,
    COALESCE(b.environment,'unattributed') AS environment,v.vendor,CAST(NULL AS STRING) AS model,
    'ledger_expense' AS cost_basis,IF(b.line_id IS NULL,'unattributed','invoice_ledger_match') AS attribution_method,
    SUM(CAST(e.amount AS NUMERIC)) AS cost_usd,
    0 AS prompt_tokens, 0 AS completion_tokens, 0 AS total_tokens
  FROM `data-warehouse-475122.accounting.fct_expenses` e
  JOIN {{ ref('infra_hosting_vendors') }} v USING (counterparty)
  LEFT JOIN {{ ref('infra_hosting_ledger_bindings') }} b USING(entry_id,line_id)
  GROUP BY 1,2,3,4,5,6,7,8
)
SELECT * FROM llm
UNION ALL SELECT * FROM hosting
UNION ALL
SELECT month,project,service,environment,vendor,CAST(NULL AS STRING) model,
 'hosting_allocation' cost_basis,attribution_method,SUM(cost_usd) cost_usd,
 0 prompt_tokens,0 completion_tokens,0 total_tokens
FROM {{ ref('fct_hosting_resource_monthly') }} GROUP BY 1,2,3,4,5,6,7,8
