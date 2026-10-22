WITH latest AS (
 SELECT event_date,vendor,cost_basis,MAX(snapshot_at) snapshot_at
 FROM {{ source('infra','infra_provider_events') }} GROUP BY 1,2,3
), expected AS (
 SELECT e.event_date d,e.vendor,e.cost_basis,
   SUM(e.cost_usd) cost_usd,SUM(e.prompt_tokens+e.completion_tokens) total_tokens
 FROM {{ source('infra','infra_provider_events') }} e
 JOIN latest USING(event_date,vendor,cost_basis,snapshot_at)
 GROUP BY 1,2,3
), actual AS (
 SELECT d,vendor,cost_basis,SUM(cost_usd) cost_usd,SUM(total_tokens) total_tokens
 FROM {{ ref('fct_infrastructure_cost_daily') }} GROUP BY 1,2,3
)
SELECT e.* FROM expected e LEFT JOIN actual a USING(d,vendor,cost_basis)
WHERE ABS(COALESCE(e.cost_usd,0)-COALESCE(a.cost_usd,0)) > 0.000001
 OR COALESCE(e.total_tokens,0) != COALESCE(a.total_tokens,0)
