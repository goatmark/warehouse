{{ config(materialized='table', schema='dwh_reporting') }}
WITH resources AS (
 SELECT month,billing_vendor vendor,SUM(cost_usd) provider_resource_cost_usd,
 SUM(usage_cost_usd) provider_effective_usage_cost_usd,
 SUM(IF(project NOT IN ('shared','unattributed'),cost_usd,0)) ownership_allocated_usd,
 SUM(IF(project='shared',cost_usd,0)) shared_pool_usd,
 SUM(IF(project='unattributed',cost_usd,0)) resource_unattributed_usd,
 STRING_AGG(DISTINCT source_kind,',') provider_source_kinds,
 STRING_AGG(DISTINCT vendor,',') resource_vendors,
 STRING_AGG(DISTINCT allocation_method,',') period_allocation_methods,
 STRING_AGG(DISTINCT evidence_url,'\n' LIMIT 20) evidence_urls
 FROM {{ ref('fct_hosting_resource_monthly') }} GROUP BY 1,2
), ledger AS (
 SELECT DATE_TRUNC(e.entry_date,MONTH) month,v.vendor,SUM(CAST(e.amount AS NUMERIC)) ledger_expense_usd
 FROM `data-warehouse-475122.accounting.fct_expenses` e
 JOIN {{ ref('infra_hosting_vendors') }} v USING(counterparty) GROUP BY 1,2
)
SELECT COALESCE(r.month,l.month) month,COALESCE(r.vendor,l.vendor) vendor,
 r.provider_resource_cost_usd,r.provider_effective_usage_cost_usd,l.ledger_expense_usd,r.ownership_allocated_usd,r.shared_pool_usd,r.resource_unattributed_usd,
 IF(r.vendor IS NOT NULL AND l.vendor IS NOT NULL,l.ledger_expense_usd-r.provider_resource_cost_usd,NULL) ledger_minus_provider_usd,
 CASE WHEN r.vendor IS NULL THEN 'resource_export_missing'
      WHEN l.vendor IS NULL THEN 'resource_cost_no_ledger_period_match'
      WHEN ABS(l.ledger_expense_usd-r.provider_resource_cost_usd)<0.005 THEN 'amounts_match_period_not_invoice_matched'
      ELSE 'unreconciled_period_amount_difference' END reconciliation_status,
 r.provider_source_kinds,r.resource_vendors,r.period_allocation_methods,r.evidence_urls
FROM resources r FULL JOIN ledger l USING(month,vendor)
