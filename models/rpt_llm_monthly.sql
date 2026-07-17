{{ config(materialized='table', schema='dwh_reporting') }}

SELECT
  FORMAT_DATE('%Y-%m', d) AS month,
  vendor,
  model,
  SUM(prompt_tokens) AS prompt_tokens,
  SUM(completion_tokens) AS completion_tokens,
  SUM(total_tokens) AS total_tokens,
  SUM(cached_tokens) AS cached_tokens,
  SUM(cache_creation_tokens) AS cache_creation_tokens,
  SUM(cost) AS cost,
  SUM(requests) AS requests,
  LOGICAL_OR(has_token_data) AS has_any_token_data
FROM `data-warehouse-475122.data_finance_business.fct_llm_usage_daily`
GROUP BY 1, 2, 3
