{{ config(materialized='table', schema='dwh_reporting') }}
WITH latest AS (
 SELECT * FROM {{ source('infra','infra_hosting_resource_events') }}
 QUALIFY ROW_NUMBER() OVER(PARTITION BY vendor,source_id,resource_id ORDER BY snapshot_at DESC)=1
), observed AS (
 SELECT vendor,MAX(snapshot_at) last_collected_at,MIN(period_start) first_source_period,
  MAX(period_end) latest_source_period,COUNT(DISTINCT resource_id) resources,
  STRING_AGG(DISTINCT source_kind,',') source_kinds
 FROM latest GROUP BY 1
), gcp AS (
 SELECT COUNT(*) expected_accounts,COUNTIF(native_table IS NOT NULL) exported_accounts,
  COUNTIF(coverage_status!='recent_native_usage_observed') partial_accounts,
  STRING_AGG(CONCAT(billing_account_id,':',coverage_status),'; ' ORDER BY billing_account_id) account_status,
  MAX(latest_native_usage) latest_native_usage
 FROM {{ ref('rpt_gcp_billing_export_coverage') }}
)
SELECT v.vendor,o.last_collected_at,o.first_source_period,o.latest_source_period,
 COALESCE(o.resources,0) resources,o.source_kinds,
 CASE WHEN o.vendor IS NULL THEN 'resource_export_pending'
      WHEN v.vendor='google_cloud' AND g.partial_accounts>0 THEN 'native_export_partial'
      WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),o.last_collected_at,HOUR)>72 THEN 'collector_stale'
      ELSE 'collected' END collection_status,
 CASE WHEN o.vendor IS NULL THEN 'Native resource export pending; ledger cash remains separately visible with evidenced ownership where available.'
      WHEN v.vendor='google_cloud' THEN CONCAT('Native accounts ',CAST(g.exported_accounts AS STRING),'/',CAST(g.expected_accounts AS STRING),
       '; latest actual usage ',COALESCE(CAST(g.latest_native_usage AS STRING),'unknown'),'; ',g.account_status,
       '. Partial backfill is not complete monthly coverage or zero missing-period spend.')
      ELSE 'Native source windows differ from payment dates; consult hosting reconciliation. Effective usage is separate from billed cost.' END coverage_note
FROM {{ ref('infra_hosting_vendors') }} v LEFT JOIN observed o USING(vendor) CROSS JOIN gcp g
