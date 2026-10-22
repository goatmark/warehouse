WITH source AS (
 SELECT TO_HEX(SHA256(CONCAT(vendor,'|',source_id,'|',resource_id))) record_id,amount_usd
 FROM {{ source('infra','infra_hosting_resource_events') }}
 QUALIFY ROW_NUMBER() OVER(PARTITION BY vendor,source_id,resource_id ORDER BY snapshot_at DESC)=1
), allocated AS (
 SELECT record_id,SUM(cost_usd) amount_usd FROM {{ ref('fct_hosting_resource_monthly') }} GROUP BY 1
)
SELECT s.* FROM source s LEFT JOIN allocated a USING(record_id)
WHERE a.record_id IS NULL OR ABS(s.amount_usd-a.amount_usd)>0.000000001
