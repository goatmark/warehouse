{{ config(materialized='table', schema='data_finance_business') }}

WITH latest_snapshots AS (
  SELECT event_date, vendor, cost_basis, MAX(snapshot_at) AS snapshot_at
  FROM {{ source('infra', 'infra_provider_events') }}
  GROUP BY 1,2,3
), current_events AS (
  SELECT e.* FROM {{ source('infra', 'infra_provider_events') }} e
  JOIN latest_snapshots s USING(event_date,vendor,cost_basis,snapshot_at)
  WHERE NOT e.is_marker
), attributed AS (
  SELECT e.event_date AS d, e.vendor, e.model, e.cost_basis,
    COALESCE(b.project,'unattributed') AS project,
    COALESCE(b.service,'unattributed') AS service,
    COALESCE(b.environment,'unattributed') AS environment,
    IF(b.credential_id IS NULL,'unattributed','credential') AS attribution_method,
    e.cost_usd, e.prompt_tokens, e.completion_tokens,
    e.prompt_tokens + e.completion_tokens AS total_tokens
  FROM current_events e
  LEFT JOIN {{ ref('infra_credential_bindings') }} b
    ON b.vendor=e.vendor AND b.credential_id=e.credential_id
    AND e.event_date >= b.valid_from
    AND e.event_date < COALESCE(b.valid_to,DATE '9999-12-31')
), legacy AS (
  SELECT d,vendor,model,
    CASE source WHEN 'receipt' THEN 'receipt_payment'
      WHEN 'provider_billed' THEN 'billed_cost' ELSE 'provider_usage' END AS cost_basis,
    'unattributed' AS project,'unattributed' AS service,
    'unattributed' AS environment,'unattributed' AS attribution_method,
    CAST(cost AS NUMERIC) AS cost_usd,prompt_tokens,completion_tokens,total_tokens
  FROM {{ ref('fct_llm_usage_daily') }}
)
SELECT * FROM attributed
UNION ALL
SELECT l.* FROM legacy l
WHERE NOT EXISTS (
  SELECT 1 FROM latest_snapshots s
  WHERE s.event_date=l.d AND s.vendor=l.vendor AND s.cost_basis=l.cost_basis
)
