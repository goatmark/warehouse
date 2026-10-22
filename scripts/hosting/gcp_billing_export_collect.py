"""Read exact native GCP billing exports; absent exports are pending, never zero cost."""
import argparse
import json
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', default='data-warehouse-475122.raw_gcp_billing')
    parser.add_argument('--credentials-file')
    parser.add_argument('--output', required=True)
    parser.add_argument('--ownership-output', required=True)
    parser.add_argument('--readiness-output', required=True)
    parser.add_argument('--raw-dir', required=True)
    args = parser.parse_args()
    from google.cloud import bigquery
    from google.oauth2 import service_account
    credentials = service_account.Credentials.from_service_account_file(args.credentials_file) if args.credentials_file else None
    client = bigquery.Client(project=args.dataset.split('.')[0], credentials=credentials)
    tables = [t for t in client.list_tables(args.dataset) if t.table_id.startswith('gcp_billing_export_v1_')]
    now = datetime.now(timezone.utc).isoformat()
    rows, owners, raw_rows = [], {}, []
    for table in tables:
        full = f'{table.project}.{table.dataset_id}.{table.table_id}'
        query = f'''
          SELECT billing_account_id, project.id AS cloud_project_id,
                 DATE_TRUNC(DATE(usage_start_time), MONTH) AS usage_month,
                 MIN(usage_start_time) AS first_usage,
                 CAST(SUM(CAST(cost AS NUMERIC)) +
                      SUM(COALESCE((SELECT SUM(CAST(c.amount AS NUMERIC)) FROM UNNEST(credits) c),0)) AS STRING) AS amount_usd,
                 ANY_VALUE(currency) AS currency,
                 ARRAY_AGG(DISTINCT service.description IGNORE NULLS) AS cloud_services
          FROM `{full}`
          GROUP BY billing_account_id, cloud_project_id, usage_month
        '''
        results = client.query(query, job_config=bigquery.QueryJobConfig(maximum_bytes_billed=100_000_000)).result()
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
    readiness = {'provider': 'google_cloud', 'dataset': args.dataset, 'state': status,
                 'native_tables': [t.table_id for t in tables], 'snapshot_at': now,
                 'note': 'No table/rows means pending coverage, not zero spend. Historical console rounded displays are evidence only and are not added to monetary facts.'}
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
