{{ config(materialized='table', schema='dwh_reporting') }}
WITH latest AS (
 SELECT * FROM {{ source('infra','infra_hosting_resource_events') }}
 QUALIFY ROW_NUMBER() OVER(PARTITION BY vendor,source_id,resource_id ORDER BY snapshot_at DESC)=1
), observed AS (
 SELECT vendor,MAX(snapshot_at) last_collected_at,MIN(period_start) first_source_period,
  MAX(period_end) latest_source_period,COUNT(DISTINCT resource_id) resources,
  STRING_AGG(DISTINCT source_kind,',') source_kinds
 FROM latest GROUP BY 1
)
SELECT v.vendor,o.last_collected_at,o.first_source_period,o.latest_source_period,
 COALESCE(o.resources,0) resources,o.source_kinds,
 CASE WHEN o.vendor IS NULL THEN 'resource_export_pending'
      WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),o.last_collected_at,HOUR)>72 THEN 'collector_stale'
      ELSE 'collected' END collection_status,
 IF(o.vendor IS NULL,'Native resource export pending; ledger cash remains separately visible with evidenced ownership where available.',
    'Native source windows differ from payment dates; consult hosting reconciliation. Effective usage is separate from billed cost.') coverage_note
FROM {{ ref('infra_hosting_vendors') }} v LEFT JOIN observed o USING(vendor)
