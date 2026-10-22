{{ config(materialized='table', schema='dwh_reporting') }}

SELECT DATE(`at`) d,vendor,project,service,environment,credential_id,run_id,generator,stage,cell,model,provider,
 IF(estimated,'meter_estimate','meter_reported_usage') cost_basis,
 COUNT(*) requests,SUM(cost_usd) diagnostic_cost_usd,
 SUM(prompt_tokens) prompt_tokens,SUM(completion_tokens) completion_tokens,SUM(reasoning_tokens) reasoning_tokens
FROM {{ ref('fct_infra_operational_usage') }} GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
