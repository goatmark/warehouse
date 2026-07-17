{{ config(materialized='table', schema='dwh_reporting') }}

SELECT
  FORMAT_DATE('%Y-%m', d) AS month,
  vendor,
  model,
  SUM(prompt_tokens) AS prompt_tokens,
  SUM(completion_tokens) AS completion_tokens,
  SUM(total_tokens) AS total_tokens,
  SUM(cost) AS cost,
  SAFE_DIVIDE(SUM(cost), SUM(total_tokens)) * 1000000 AS cost_per_million_tokens,
  SAFE_DIVIDE(SUM(cost), SUM(prompt_tokens)) * 1000000 AS input_cost_per_million,
  SAFE_DIVIDE(SUM(cost), SUM(completion_tokens)) * 1000000 AS output_cost_per_million
FROM `data-warehouse-475122.data_finance_business.fct_llm_usage_daily`
WHERE has_token_data = TRUE
GROUP BY 1, 2, 3
HAVING SUM(total_tokens) > 0
