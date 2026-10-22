SELECT entry_id,line_id FROM {{ ref('infra_hosting_ledger_bindings') }} GROUP BY 1,2 HAVING COUNT(*)>1
