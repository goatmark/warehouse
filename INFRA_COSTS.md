# Infrastructure costs

`dwh_reporting.rpt_infrastructure_cost_monthly` is the financial-sheet source.
Its dimensions are month, project, service, environment, vendor, model,
cost_basis and attribution_method. Amounts are USD NUMERIC. NULL cost means
unavailable, not free. NULL month identifies invalid historical source dates.

| Cost basis | Meaning | Additive with another basis? |
|---|---|---|
| provider_usage | OpenRouter consumed cost and provider token counters | No |
| billed_cost | OpenAI and Anthropic provider cost reports, not invoice payment | No |
| receipt_payment | Legacy email/card receipts, including API credits and subscriptions; not reconciled or guaranteed deduplicated | No |
| ledger_expense | Railway, Vercel, Supabase, Google Cloud and DigitalOcean expense lines from the accounting ledger | No |

Receipts are excluded from the existing `rpt_llm_monthly` consumption report.
Legacy OpenAI model rows have unknown cost because the collector copied the
organization's daily cost to every model. Its newest day cost appears once on
a model=NULL billed_cost row. OpenAI cached input is a subset of input tokens.
Changed token counts no longer form new deduplication identities.

## Credential attribution

`seeds/infra_credential_bindings.csv` is the authoritative warehouse map from
vendor and nonsecret provider key ID/hash to project, service and environment.
It holds no secrets. Half-open valid_from/valid_to dates express ownership,
starting with creation date. A binding does not assert deployment or cutover.
Secret paths and rollout status are maintained separately under
`H:/My Drive/Infra/openrouter/service-keys/manifest-2026-10-22.json`.
Import additional manifest metadata with `python scripts/import_infra_bindings.py
<manifest-path> --valid-from 2026-10-22`, review the seed and run the dbt build.
The explicit date is needed only when created_at is missing. Existing ownership
rows are preserved; rotation creates a new provider ID rather than overwriting it.

Unknown or ambiguous IDs remain unattributed. Shared historical keys cannot
prove a product allocation. Generator, stage and run IDs belong in workload
logs; they do not require more keys. Native provider totals replace, rather
than add to, overlapping legacy day/basis rows. Agent logs are diagnostic
evidence and are never added to provider aggregate spend.

## Collection and maintenance

`scripts/infra_cost_sync.py --vendor openrouter --days 7` reads the provider's
management API and appends a complete daily snapshot into
`data_finance_business.infra_provider_events`. Vendor options are openai,
anthropic and openrouter. --start/--end support explicit UTC dates; end is
exclusive. Defaults cover seven completed UTC days. Snapshot markers include
empty days so corrections remove stale rows. Pagination loops and truncated
one-day results fail visibly. Raw snapshots remain append-only.

`.github/workflows/infra-cost-sync.yml` runs daily at 06:15 UTC. Provider jobs
run independently, and the report build waits for all of them to succeed.
The reporting repository holds admin/management keys in GitHub Actions secrets,
separate from inference keys. Local fallback paths are in H:/My Drive/Infra.
The existing warehouse build also rebuilds these dbt models every two hours.

OpenAI cost reports cover all cost line items; token counters currently cover
completions and embeddings, not images/audio/storage. Anthropic costs are
workspace/description-grained and cannot be claimed as exact key-level spend;
Priority Tier costs are absent from that API. Its usage remains key-grained.
OpenRouter groups by key and model; returned labels are mapped only when the
management inventory gives a unique label-to-hash match. Deleted/ambiguous
keys remain unattributed.

Hosting coverage uses accounting ledger expenses. Resource/project-level
Railway, Vercel, Supabase and GCP usage exports are not present, so those costs
remain unattributed. Google Cloud includes all cloud products; it cannot be
labelled specifically Cloud Run. Ledger classification changes belong in the
ledger repository. Do not infer hosting project ownership from vendor names.

Verify changes with `python -m unittest discover -s tests -p 'test_*.py'` and
`dbt build --profiles-dir . --select +rpt_infrastructure_cost_monthly rpt_llm_monthly rpt_llm_cost_per_token`.
Database tests prevent overlapping ownership windows, mixed receipt/usage
monthly totals and any multiplication of native provider cost/token totals.
