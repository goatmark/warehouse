SELECT a.vendor,a.resource_id
FROM {{ ref('infra_hosting_resource_bindings') }} a
JOIN {{ ref('infra_hosting_resource_bindings') }} b
 ON a.vendor=b.vendor AND a.resource_id=b.resource_id
 AND a.valid_from<COALESCE(b.valid_to,DATE '9999-12-31')
 AND b.valid_from<COALESCE(a.valid_to,DATE '9999-12-31')
GROUP BY 1,2,a.valid_from HAVING COUNT(*)>1
