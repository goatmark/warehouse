{{ config(materialized='table', schema='data_finance_business') }}

WITH or_dedup AS (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY DATE(ts), model, ts, prompt_tokens, completion_tokens
    ORDER BY loaded_at DESC
  ) AS rn
  FROM `data-warehouse-475122.data_finance_business.fct_openrouter_requests`
),
ant_dedup AS (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY DATE(bucket_start), model, bucket_start, input_tokens, output_tokens
    ORDER BY loaded_at DESC
  ) AS rn
  FROM `data-warehouse-475122.data_finance_business.fct_anthropic_usage_daily`
),
oai_dedup AS (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY DATE(bucket_start), model, bucket_start, input_tokens, output_tokens
    ORDER BY loaded_at DESC
  ) AS rn
  FROM `data-warehouse-475122.data_finance_business.fct_openai_usage_daily`
),

or_agg AS (
  SELECT
    DATE(ts) AS d, 'openrouter' AS vendor, model,
    SUM(prompt_tokens) AS prompt_tokens, SUM(completion_tokens) AS completion_tokens,
    SUM(total_tokens) AS total_tokens, SUM(cached_tokens) AS cached_tokens,
    0 AS cache_creation_tokens, SUM(cost) AS cost, COUNT(*) AS requests,
    TRUE AS has_token_data, 'api_usage' AS source
  FROM or_dedup WHERE rn = 1
  GROUP BY 1, 2, 3
),
ant_agg AS (
  SELECT
    DATE(bucket_start) AS d, 'anthropic' AS vendor, model,
    SUM(input_tokens) AS prompt_tokens, SUM(output_tokens) AS completion_tokens,
    SUM(input_tokens + output_tokens + COALESCE(cache_creation_tokens,0) + COALESCE(cached_input_tokens,0)) AS total_tokens,
    SUM(cached_input_tokens) AS cached_tokens, SUM(cache_creation_tokens) AS cache_creation_tokens,
    SUM(cost_usd) AS cost, NULL AS requests, TRUE AS has_token_data, 'api_usage' AS source
  FROM ant_dedup WHERE rn = 1
  GROUP BY 1, 2, 3
),
oai_agg AS (
  SELECT
    DATE(bucket_start) AS d, 'openai' AS vendor, model,
    SUM(input_tokens) AS prompt_tokens, SUM(output_tokens) AS completion_tokens,
    SUM(input_tokens + output_tokens + COALESCE(cached_tokens,0)) AS total_tokens,
    SUM(cached_tokens) AS cached_tokens, 0 AS cache_creation_tokens,
    SUM(cost_usd) AS cost, SUM(requests) AS requests, TRUE AS has_token_data, 'api_usage' AS source
  FROM oai_dedup WHERE rn = 1
  GROUP BY 1, 2, 3
),
receipts AS (
  SELECT
    d, vendor, CAST(NULL AS STRING) AS model,
    0 AS prompt_tokens, 0 AS completion_tokens, 0 AS total_tokens,
    0 AS cached_tokens, 0 AS cache_creation_tokens,
    total AS cost, CAST(NULL AS INT64) AS requests, FALSE AS has_token_data, 'receipt' AS source
  FROM `data-warehouse-475122.data_finance_business.fct_llm_receipts`
)

SELECT * FROM or_agg
UNION ALL
SELECT * FROM ant_agg
UNION ALL
SELECT * FROM oai_agg
UNION ALL
SELECT * FROM receipts
