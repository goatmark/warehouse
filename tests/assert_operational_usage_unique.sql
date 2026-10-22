SELECT record_id FROM {{ ref('fct_infra_operational_usage') }} GROUP BY 1 HAVING COUNT(*)>1
