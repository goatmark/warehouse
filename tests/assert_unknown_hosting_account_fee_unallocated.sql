SELECT f.* FROM {{ ref('fct_hosting_resource_monthly') }} f
WHERE f.cost_kind='account_fee' AND f.project!='unattributed'
AND NOT EXISTS (
 SELECT 1 FROM {{ ref('infra_hosting_resource_bindings') }} b
 WHERE b.vendor=f.vendor AND b.resource_id=f.resource_id
)
