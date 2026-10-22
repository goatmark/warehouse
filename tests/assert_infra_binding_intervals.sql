SELECT a.vendor,a.credential_id
FROM {{ ref('infra_credential_bindings') }} a
JOIN {{ ref('infra_credential_bindings') }} b
 ON a.vendor=b.vendor AND a.credential_id=b.credential_id
 AND a.valid_from < COALESCE(b.valid_to,DATE '9999-12-31')
 AND b.valid_from < COALESCE(a.valid_to,DATE '9999-12-31')
GROUP BY 1,2,a.valid_from
HAVING COUNT(*) > 1
