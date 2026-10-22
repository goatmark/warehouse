"""Read exact native GCP billing exports; absent exports are pending, never zero cost."""
import argparse
import csv
import json
import os
from decimal import Decimal
from datetime import datetime, timezone
from pathlib import Path

OWNERS = {
    'data-warehouse-475122': ('shared', 'warehouse'),
    'prepsavvy-prod': ('prepsavvy', 'cloud'),
    'i-academy-502921-u1': ('prepsavvy', 'cloud'),
    'bibletimes-infra': ('bibletimes', 'cloud'),
    'agent-testing-455721': ('unattributed', 'unattributed'),
    'drive-access-443720': ('shared', 'connectors'),
    'claude-mcp-gmail-491123': ('shared', 'gmail-connector'),
}


def account_status(native_table,latest,dataset_state,now):
    if latest:
        lag=(datetime.fromisoformat(now)-datetime.fromisoformat(str(latest))).total_seconds()
        return 'backfill_or_source_lag' if lag>72*3600 else 'recent_native_usage_observed'
    if native_table:return 'awaiting_native_rows'
    if dataset_state=='reporting_credential_required':return dataset_state
    return 'awaiting_native_table'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', action='append', help='Repeatable override; default reads registered export datasets')
    parser.add_argument('--query-project', default='data-warehouse-475122')
    parser.add_argument('--credentials-file')
    parser.add_argument('--write-readiness', action='store_true')
    parser.add_argument('--output', required=True)
    parser.add_argument('--ownership-output', required=True)
    parser.add_argument('--readiness-output', required=True)
    parser.add_argument('--raw-dir', required=True)
    args = parser.parse_args()
    from google.cloud import bigquery
    import google.auth
    credentials = google.auth.load_credentials_from_file(args.credentials_file,scopes=['https://www.googleapis.com/auth/cloud-platform'])[0] if args.credentials_file else None
    client = bigquery.Client(project=args.query_project, credentials=credentials)
    with (Path(__file__).parents[2]/'seeds/infra_gcp_billing_accounts.csv').open(newline='',encoding='utf-8') as stream:
        expected=list(csv.DictReader(stream))
    datasets=args.dataset or sorted({r['export_project']+'.'+r['export_dataset'] for r in expected})
    from google.api_core.exceptions import NotFound,Forbidden
    tables=[];dataset_states=[];accounts=set()
    for dataset in datasets:
        source_client=client
        if dataset.startswith('i-academy-502921-u1.'):
            import google.auth
            source_file=os.environ.get('GCP_PREPSAVVY_CREDENTIALS_FILE')
            local=Path('H:/My Drive/Infra/gcp-prepsavvy-admin/credentials.json')
            if not source_file and local.exists():source_file=str(local)
            if source_file:
                source_credentials,_=google.auth.load_credentials_from_file(source_file,scopes=['https://www.googleapis.com/auth/cloud-platform'])
                source_client=bigquery.Client(project=dataset.split('.')[0],credentials=source_credentials)
        try:
            native=[t for t in source_client.list_tables(dataset) if t.table_id.startswith('gcp_billing_export_v1_')]
        except NotFound:
            dataset_states.append({'dataset':dataset,'state':'awaiting_dataset'});continue
        except Forbidden:
            dataset_states.append({'dataset':dataset,'state':'reporting_credential_required'});continue
        dataset_states.append({'dataset':dataset,'state':'native_tables_present' if native else 'awaiting_native_table'})
        for table in native:
            account=table.table_id.removeprefix('gcp_billing_export_v1_')
            if account in accounts:raise RuntimeError('Duplicate billing account export across datasets; choose one authoritative source')
            accounts.add(account);tables.append((table,source_client))
    now = datetime.now(timezone.utc).isoformat()
    rows, owners, raw_rows = [], {}, []
    for table,source_client in tables:
        full = f'{table.project}.{table.dataset_id}.{table.table_id}'
        query = f'''
          SELECT billing_account_id, project.id AS cloud_project_id,
                 DATE_TRUNC(DATE(usage_start_time), MONTH) AS usage_month,
                 MIN(usage_start_time) AS first_usage,
                 MAX(usage_end_time) AS last_usage,
                 MAX(export_time) AS last_export,
                 COUNT(*) AS native_rows,
                 CAST(SUM(CAST(cost AS NUMERIC)) +
                      SUM(COALESCE((SELECT SUM(CAST(c.amount AS NUMERIC)) FROM UNNEST(credits) c),0)) AS STRING) AS amount_usd,
                 ANY_VALUE(currency) AS currency,
                 ARRAY_AGG(DISTINCT service.description IGNORE NULLS) AS cloud_services,
                 ARRAY_AGG(DISTINCT sku.description IGNORE NULLS) AS cloud_skus
          FROM `{full}`
          GROUP BY billing_account_id, cloud_project_id, usage_month
        '''
        results = source_client.query(query, job_config=bigquery.QueryJobConfig(maximum_bytes_billed=100_000_000)).result()
        for result in results:
            item = dict(result)
            project = item['cloud_project_id']
            resource = f'project:{project}' if project else f"billing_account:{item['billing_account_id']}"
            start = item['usage_month']
            end = start.replace(year=start.year + 1, month=1) if start.month == 12 else start.replace(month=start.month + 1)
            end_at = min(datetime(end.year, end.month, end.day, tzinfo=timezone.utc).isoformat(), now)
            evidence = f'https://console.cloud.google.com/bigquery?project={table.project}&p={table.project}&d={table.dataset_id}&t={table.table_id}&page=table'
            if item['currency'] != 'USD':
                raise RuntimeError('Non-USD native billing export needs explicit currency conversion')
            rows.append({'vendor': 'google_cloud', 'resource_id': resource,
                'resource_name': project or 'Billing account adjustments',
                'billing_service': ', '.join(sorted(item['cloud_services'])),
                'period_start': start.isoformat(), 'period_end': end_at,
                'amount_usd': item['amount_usd'], 'currency': item['currency'],
                'cost_kind': 'resource_usage' if project else 'invoice_adjustment',
                'source_kind': 'export',
                'source_id': f"gcp:{item['billing_account_id']}:{start}:{resource}",
                'evidence_url': evidence, 'snapshot_at': now})
            owner, service = OWNERS.get(project, ('unattributed', 'unattributed'))
            owners[resource] = {'vendor': 'google_cloud', 'resource_id': resource, 'project': owner,
                'service': service, 'environment': 'production' if project and project.endswith('-prod') else 'mixed', 'valid_from': '2026-01-01',
                'valid_to': None, 'evidence_url': evidence}
            raw_rows.append({k: str(v) if hasattr(v, 'isoformat') else v for k, v in item.items()})
    status = 'native_export_available' if rows else 'awaiting_native_export_rows' if tables else 'awaiting_first_native_export_table'
    account_readiness=[]
    for expected_account in expected:
        account=expected_account['billing_account_id'];observed=[r for r in raw_rows if r['billing_account_id']==account]
        native_table=next((f'{t.project}.{t.dataset_id}.{t.table_id}' for t,_ in tables if t.table_id.endswith(account.replace('-','_'))),None)
        dataset=expected_account['export_project']+'.'+expected_account['export_dataset']
        dataset_state=next((d['state'] for d in dataset_states if d['dataset']==dataset),'not_collected')
        latest=max((r['last_usage'] for r in observed),default=None)
        state=account_status(native_table,latest,dataset_state,now)
        account_readiness.append({'billing_account_id':account,'snapshot_at':now,'native_table':native_table,
            'native_rows':sum(r['native_rows'] for r in observed),'first_native_usage':min((r['first_usage'] for r in observed),default=None),
            'latest_native_usage':latest,'last_native_export':max((r['last_export'] for r in observed),default=None),
            'native_net_cost_usd':str(sum((Decimal(r['amount_usd']) for r in observed),Decimal(0))) if observed else None,'coverage_status':state})
    readiness = {'provider': 'google_cloud', 'datasets': dataset_states, 'state': status,
                 'native_tables': [f'{t.project}.{t.dataset_id}.{t.table_id}' for t,_ in tables], 'snapshot_at': now,
                 'accounts':account_readiness,
                 'first_native_usage': str(min((r['first_usage'] for r in raw_rows),default=None)),
                 'latest_native_usage': str(max((r['last_usage'] for r in raw_rows),default=None)),
                 'expected_accounts': [r['billing_account_id'] for r in expected],
                 'note': 'Exports may be partially backfilling. Missing tables/periods are not zero spend or complete monthly coverage. Historical rounded console displays are not monetary facts.'}
    if args.write_readiness:
        schema=[bigquery.SchemaField(n,t) for n,t in [('billing_account_id','STRING'),('snapshot_at','TIMESTAMP'),('native_table','STRING'),('native_rows','INT64'),('first_native_usage','TIMESTAMP'),('latest_native_usage','TIMESTAMP'),('last_native_export','TIMESTAMP'),('native_net_cost_usd','NUMERIC'),('coverage_status','STRING')]]
        target='data-warehouse-475122.data_finance_business.infra_gcp_export_readiness'
        client.create_table(bigquery.Table(target,schema=schema),exists_ok=True)
        client.load_table_from_json(account_readiness,target,job_config=bigquery.LoadJobConfig(schema=schema,write_disposition='WRITE_APPEND')).result()
    for file, values in [(args.output, rows), (args.ownership_output, list(owners.values()))]:
        Path(file).parent.mkdir(parents=True, exist_ok=True)
        Path(file).write_text(''.join(json.dumps(row) + '\n' for row in values), encoding='utf-8')
    Path(args.readiness_output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.readiness_output).write_text(json.dumps(readiness, indent=2), encoding='utf-8')
    raw_dir = Path(args.raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / 'native-month-project.json').write_text(json.dumps(raw_rows, indent=2), encoding='utf-8')
    print(json.dumps(readiness))


if __name__ == '__main__':
    main()
