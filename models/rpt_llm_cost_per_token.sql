{{ config(materialized='table', schema='dwh_reporting') }}

WITH monthly AS (
  SELECT
    FORMAT_DATE('%Y-%m', d) AS month,
    vendor,
    model,
    SUM(prompt_tokens) AS prompt_tokens,
    SUM(completion_tokens) AS completion_tokens,
    SUM(total_tokens) AS total_tokens,
    SUM(cost) AS cost
  FROM `data-warehouse-475122.data_finance_business.fct_llm_usage_daily`
  WHERE has_token_data = TRUE
  GROUP BY 1, 2, 3
)
SELECT
  month, vendor, model,
  prompt_tokens, completion_tokens, total_tokens, cost,
  CASE WHEN total_tokens > 0 THEN ROUND(cost / total_tokens * 1000000, 4) ELSE NULL END AS cost_per_million_tokens,
  CASE WHEN prompt_tokens > 0 THEN ROUND(cost / prompt_tokens * 1000000, 4) ELSE NULL END AS input_cost_per_million,
  CASE WHEN completion_tokens > 0 THEN ROUND(cost / completion_tokens * 1000000, 4) ELSE NULL END AS output_cost_per_million
FROM monthly
WHERE total_tokens > 0
