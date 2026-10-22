WITH expected AS (
 SELECT DATE_TRUNC(e.entry_date,MONTH) month,v.vendor,SUM(CAST(e.amount AS NUMERIC)) amount
 FROM `data-warehouse-475122.accounting.fct_expenses` e
 JOIN {{ ref('infra_hosting_vendors') }} v USING(counterparty) GROUP BY 1,2
), reported AS (
 SELECT month,vendor,SUM(cost_usd) amount FROM {{ ref('rpt_infrastructure_cost_monthly') }}
 WHERE cost_basis='ledger_expense' GROUP BY 1,2
)
SELECT e.* FROM expected e FULL JOIN reported r USING(month,vendor)
WHERE ABS(COALESCE(e.amount,0)-COALESCE(r.amount,0))>0.000000001
