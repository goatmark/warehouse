SELECT month,vendor FROM {{ ref('rpt_hosting_reconciliation_monthly') }}
WHERE ABS(COALESCE(provider_resource_cost_usd,0)-COALESCE(ownership_allocated_usd,0)
  -COALESCE(shared_pool_usd,0)-COALESCE(resource_unattributed_usd,0))>0.000000001
