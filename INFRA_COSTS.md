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
| hosting_allocation | Native resource billing/usage dollars allocated by evidenced resource ownership and exact billing-period time overlap | No |

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
workspace/description-grained and cannot be claimed as exact key-level spend.
`seeds/infra_provider_project_bindings.csv` explicitly maps dedicated provider
projects/workspaces to project and environment using date windows. Workspace-
only bills use attribution_method=provider_project, while service remains
unattributed. Existing default/shared provider workspaces remain unmapped.
Priority Tier costs are absent from that API. Its usage remains key-grained.
OpenAI billed costs retain native API key IDs as well as project and line item,
so registered service keys give exact service-level billed attribution. A native
30-day comparison conserved all $76.31920124 before and after key grouping.
OpenRouter groups by key and model; returned labels are mapped only when the
management inventory gives a unique label-to-hash match. Deleted/ambiguous
keys remain unattributed.

Hosting resources are ingested by `scripts/hosting_resource_sync.py` from native
Vercel FOCUS exports, Railway service meters, finalized DigitalOcean invoices,
and reconciled Supabase invoice lines. `infra_hosting_resource_bindings` holds
resource ownership and evidence. `fct_hosting_resource_monthly` preserves native
source IDs and billing windows, costs, effective usage costs and evidence. Billing
windows crossing months are allocated by exact time overlap, with a final-day
rounding remainder so costs are conserved. Current Railway accrual stops at its
measurement cutoff; final invoices retain their contractual service interval.

`rpt_hosting_reconciliation_monthly` keeps provider resource costs, ledger
expenses, allocated project costs, shared fees and unknown resources separate.
Its difference is a reconciliation gap, never a fictitious shared expense.
`billing_vendor` distinguishes the cash merchant from the resource provider:
Vercel-billed Supabase marketplace costs reconcile against Vercel cash while
remaining Supabase resource costs. Supabase prior balances are not added again
as expenses. Effective usage from included-credit plans is separate from billed
costs and is never added to them.

Eighteen exact cash lines match native invoice/receipt evidence via
`infra_hosting_ledger_bindings`: three BibleTimes Google Cloud payments,
ten BibleTimes Supabase settlements, and five website Supabase marketplace
settlements billed by Vercel. These are attributed within ledger_expense,
without inventing SKU usage or service periods. Other unresolved cloud costs
remain visible. `rpt_hosting_source_coverage` exposes each vendor's last collection,
source window, resource count and pending/stale status. Native GCP main-account
records have arrived, with partial historical backfill. A freshly collected
export does not prove a complete month or zero cost for missing periods.
`rpt_gcp_billing_export_coverage` separately tracks the main account, the PrepSavvy
factory account and the BibleTimes account, including native usage cutoffs and
missing-table/access states. Ledger classification changes belong in the
ledger repository.

GCP export locations are registered in `infra_gcp_billing_accounts`. The central
reporting identity reads its own account; PrepSavvy and BibleTimes accounts are
read in their own projects using dedicated keyless billing readers through GitHub OIDC. Each reader's
scope is dataset dataViewer and project jobUser; the identity provider is pinned
to this repository, main branch and exact hosting workflow. Cross-organization
domain restrictions remain intact. No broad factory service-account key is copied
to CI. Only normalized billing facts and source-readiness metadata are loaded
centrally. The local existing owner credential can verify initial export readiness.

| Billing account | Native export dataset | Reporting access |
| --- | --- | --- |
| 01A404-EDB788-70AEC0 (main/shared, including prepsavvy-prod) | data-warehouse-475122.raw_gcp_billing (US) | Existing central reporting identity |
| 01AB73-37ED3F-744977 (Prep factory) | i-academy-502921-u1.raw_gcp_billing (US) | Dedicated keyless Prep reader |
| 01E29E-5FD32D-B9B334 (BibleTimes) | bibletimes-infra.raw_gcp_billing (US) | Dedicated keyless BibleTimes reader |

All three native exports were enabled on 2026-10-22. The initial main export
contained 421 rows/$0.194549 through 2026-09-01; this is partial historical
backfill, not October coverage. Prep subsequently exported 51 historical rows
through September 1 with observed net cost zero; this does not establish zero
October costs. BibleTimes keyless dataset access is verified and its first table
remains pending. The collector detects new standard export tables automatically.
New venture exports retain native service/SKU detail; proven i-academy BigQuery
charges are assigned to PrepSavvy factory-warehouse, without inferring that
other cloud services belong to that workload.
The complete dual-reader hosted workflow and all 24 dbt checks passed on
2026-10-22. Both ephemeral OIDC credential files are ignored and explicitly
removed after collection; no venture service-account key is stored in CI.

`.github/workflows/hosting-cost-sync.yml` refreshes native hosting data daily
at 06:45 UTC in GitHub Actions. Railway uses a dedicated workspace reporting
token; DigitalOcean uses the existing billing identity. Supabase PDFs and explicit
Vercel marketplace Supabase lines are fetched through existing read-only Gmail
OAuth. Marketplace collection verifies the unique live Supabase installation,
uses actual receipt amounts/periods/IDs, and rejects ambiguous or mixed invoices.
It never projects a recurring plan charge. Vercel OAuth sessions are refreshed
through the official CLI and securely persisted back to repository secrets.
Hosted execution, all exporters, OAuth persistence, and the complete 22-check
hosting build were verified successfully on 2026-10-22.

## Generator operational diagnostics

`scripts/operational_usage_sync.py` reads only registered PrepSavvy factory meter
files or the configured exact Drive metadata-file ID. Prompts, responses, API
keys and error bodies are never persisted. Factory meter calls mirror to
`H:/My Drive/Infra/ai-cost-attribution/operational/prepsavvy-factory.jsonl` for
Drive synchronization. `.github/workflows/operational-usage-sync.yml` collects
that file hourly using the existing reporting OAuth identity. No Windows task
is installed. Existing generator processes require their normal restart before
new central logging code takes effect; no processes are killed by ingestion.

`fct_infra_operational_usage` deduplicates call metadata; its daily report retains
run, generator, stage, cell, model, provider and measured-versus-estimated costs.
Historical missing key/environment/generator values stay unknown. These
diagnostic amounts never enter financial/provider totals. Local and cloud
loaders skip already-ingested call IDs. The cloud reader uses only the allowlisted
Drive file; its OAuth identity is never exposed to generation runtimes.

Verify changes with `python -m unittest discover -s tests -p 'test_*.py'` and
`dbt build --profiles-dir . --select +rpt_infrastructure_cost_monthly rpt_llm_monthly rpt_llm_cost_per_token`.
Database tests prevent overlapping ownership windows, mixed receipt/usage
monthly totals and any multiplication of native provider cost/token totals.
