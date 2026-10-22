{{ config(materialized='table', schema='dwh_reporting') }}

-- Source identities query only their authorized datasets. Only normalized
-- readiness lands centrally; no cross-organization read grant is assumed.
WITH latest AS (
 SELECT * FROM {{ source('infra','infra_gcp_export_readiness') }}
 QUALIFY ROW_NUMBER() OVER(PARTITION BY billing_account_id ORDER BY snapshot_at DESC)=1
)
SELECT e.billing_account_id,e.account_scope,e.export_project,e.export_dataset,
 n.native_table,COALESCE(n.native_rows,0) native_rows,
 n.first_native_usage,n.latest_native_usage,n.last_native_export,n.native_net_cost_usd,
 COALESCE(n.coverage_status,'not_collected') coverage_status,n.snapshot_at last_checked_at,
 'Observed native rows are partial exports, not proof of a complete month or zero spend for missing periods.' coverage_note
FROM {{ ref('infra_gcp_billing_accounts') }} e LEFT JOIN latest n USING(billing_account_id)
