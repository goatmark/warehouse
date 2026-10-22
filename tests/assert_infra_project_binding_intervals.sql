SELECT a.vendor,a.provider_project_id
FROM {{ ref('infra_provider_project_bindings') }} a
JOIN {{ ref('infra_provider_project_bindings') }} b
 ON a.vendor=b.vendor AND a.provider_project_id=b.provider_project_id
 AND a.valid_from < COALESCE(b.valid_to,DATE '9999-12-31')
 AND b.valid_from < COALESCE(a.valid_to,DATE '9999-12-31')
GROUP BY 1,2,a.valid_from
HAVING COUNT(*) > 1
