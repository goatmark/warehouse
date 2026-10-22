WITH report AS (
 SELECT month,vendor,SUM(cost_usd) amount FROM {{ ref('rpt_infrastructure_cost_monthly') }}
 WHERE cost_basis='hosting_allocation' GROUP BY 1,2
), expected AS (
 SELECT month,vendor,SUM(cost_usd) amount FROM {{ ref('fct_hosting_resource_monthly') }} GROUP BY 1,2
)
SELECT e.* FROM expected e FULL JOIN report r USING(month,vendor)
WHERE ABS(COALESCE(e.amount,0)-COALESCE(r.amount,0))>0.000000001
