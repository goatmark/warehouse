{{ config(materialized='table', schema='data_finance_business') }}

WITH dedup AS (
 SELECT * REPLACE(TO_HEX(SHA256(CONCAT(vendor,'|',source_id,'|',resource_id))) AS record_id)
 FROM {{ source('infra','infra_hosting_resource_events') }}
 QUALIFY ROW_NUMBER() OVER(PARTITION BY vendor,source_id,resource_id ORDER BY snapshot_at DESC)=1
), days AS (
 SELECT r.*,d,
   TIMESTAMP_DIFF(LEAST(period_end,TIMESTAMP(DATE_ADD(d,INTERVAL 1 DAY))),
     GREATEST(period_start,TIMESTAMP(d)),MICROSECOND) overlap_us,
   TIMESTAMP_DIFF(period_end,period_start,MICROSECOND) duration_us
 FROM dedup r CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(DATE(period_start),DATE(TIMESTAMP_SUB(period_end,INTERVAL 1 MICROSECOND)))) d
), split AS (
 SELECT *, amount_usd*CAST(overlap_us AS NUMERIC)/CAST(duration_us AS NUMERIC) AS day_amount,
   usage_cost_usd*CAST(overlap_us AS NUMERIC)/CAST(duration_us AS NUMERIC) day_usage_cost
 FROM days
), rounded AS (
 SELECT *, IF(d=DATE(TIMESTAMP_SUB(period_end,INTERVAL 1 MICROSECOND)),
   amount_usd-SUM(IF(d<DATE(TIMESTAMP_SUB(period_end,INTERVAL 1 MICROSECOND)),day_amount,0)) OVER(PARTITION BY record_id),
   day_amount) allocated_amount,
   IF(d=DATE(TIMESTAMP_SUB(period_end,INTERVAL 1 MICROSECOND)),
   usage_cost_usd-SUM(IF(d<DATE(TIMESTAMP_SUB(period_end,INTERVAL 1 MICROSECOND)),day_usage_cost,0)) OVER(PARTITION BY record_id),
   day_usage_cost) allocated_usage_cost
 FROM split
), attributed AS (
 SELECT r.*,
   COALESCE(b.project,'unattributed') project,
   COALESCE(b.service,'unattributed') service,
   COALESCE(b.environment,'unattributed') environment,
   IF(b.resource_id IS NOT NULL,'resource_ownership','unattributed') attribution_method,
   b.evidence_url ownership_evidence_url
 FROM rounded r LEFT JOIN {{ ref('infra_hosting_resource_bindings') }} b
  ON r.vendor=b.vendor AND r.resource_id=b.resource_id
  AND r.d>=b.valid_from AND r.d<COALESCE(b.valid_to,DATE '9999-12-31')
)
SELECT DATE_TRUNC(d,MONTH) month,vendor,resource_id,resource_name,project,service,environment,
 attribution_method,cost_kind,source_kind,source_id,evidence_url,ownership_evidence_url,
 record_id,period_start,period_end,
 IF(DATE_TRUNC(DATE(period_start),MONTH)=DATE_TRUNC(DATE(TIMESTAMP_SUB(period_end,INTERVAL 1 MICROSECOND)),MONTH),
   'within_source_period','billing_period_time_proration') allocation_method,
 billing_service,COALESCE(billing_vendor,vendor) billing_vendor,
 SUM(allocated_amount) cost_usd,SUM(allocated_usage_cost) usage_cost_usd
FROM attributed GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
