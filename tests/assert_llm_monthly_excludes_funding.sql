WITH actual AS (
 SELECT month, vendor, SUM(cost) AS cost FROM {{ ref('rpt_llm_monthly') }} GROUP BY 1,2
), expected AS (
 SELECT FORMAT_DATE('%Y-%m',d) AS month, vendor, SUM(cost) AS cost
 FROM {{ ref('fct_llm_usage_daily') }} WHERE source != 'receipt' GROUP BY 1,2
)
SELECT a.month,a.vendor FROM actual a FULL JOIN expected e USING(month,vendor)
WHERE ABS(COALESCE(a.cost,0)-COALESCE(e.cost,0)) > 0.000001
