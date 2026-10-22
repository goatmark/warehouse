{{ config(materialized='table', schema='dwh_reporting') }}
WITH observed AS (
 SELECT vendor,MAX(snapshot_at) last_collected_at,MIN(period_start) first_source_period,
  MAX(period_end) latest_source_period,COUNT(DISTINCT resource_id) resources,
  STRING_AGG(DISTINCT source_kind,',') source_kinds
 FROM {{ source('infra','infra_hosting_resource_events') }} GROUP BY 1
)
SELECT v.vendor,o.last_collected_at,o.first_source_period,o.latest_source_period,
 COALESCE(o.resources,0) resources,o.source_kinds,
 CASE WHEN o.vendor IS NULL THEN 'resource_export_pending'
      WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),o.last_collected_at,HOUR)>72 THEN 'collector_stale'
      ELSE 'collected' END collection_status,
 IF(o.vendor IS NULL,'No resource dollars allocated; ledger expenses remain visible and unallocated.',
    'Native source windows differ from payment dates; consult hosting reconciliation. Effective usage is separate from billed cost.') coverage_note
FROM {{ ref('infra_hosting_vendors') }} v LEFT JOIN observed o USING(vendor)
