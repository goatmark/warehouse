"""Collect native Railway service billing meters without treating them as invoices."""
import argparse
import calendar
import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

OWNERS = {
    'c775b566-4003-4e8c-b776-7e9195c0e85c': ('jarvis', 'jarvis'),
    'a95458c2-c47d-4a00-9f66-2a0001671ecd': ('bibletimes', 'cx'),
    '6b2c3d13-23c6-4c76-9cd4-072600fb6a63': ('prepsavvy', 'ops'),
    '2f3bbf44-622c-4c7c-95cf-cbc1b1fab633': ('bibletimes', 'gtm'),
}

RATES = {'MEMORY_USAGE_GB': Decimal(10) / 43200, 'CPU_USAGE': Decimal(20) / 43200,
         'NETWORK_TX_GB': Decimal('0.05'), 'DISK_USAGE_GB': Decimal('0.15') / 43200,
         'BACKUP_USAGE_GB': Decimal('0.15') / 43200}


def graphql_json(query, variables):
    source = r'''
import fs from 'node:fs';import os from 'node:os';import path from 'node:path';
const input=JSON.parse(fs.readFileSync(0,'utf8'));
const token=process.env.RAILWAY_API_TOKEN||JSON.parse(fs.readFileSync(path.join(os.homedir(),'.railway/config.json'),'utf8')).user.accessToken;
const r=await fetch('https://backboard.railway.com/graphql/v2',{method:'POST',headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/json'},body:JSON.stringify(input)});
if(!r.ok)throw new Error(`Railway GraphQL HTTP ${r.status}`);const j=await r.json();if(j.errors)throw new Error('Railway read query failed');console.log(JSON.stringify(j.data));
'''
    result = subprocess.run([shutil.which('node') or 'node', '--input-type=module', '-e', source],
                            input=json.dumps({'query': query, 'variables': variables}), capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError('Railway billing GraphQL read failed; provider output withheld')
    return json.loads(result.stdout)


def shift_month(value, count):
    index = value.year * 12 + value.month - 1 + count
    year, month = divmod(index, 12)
    month += 1
    return value.replace(year=year, month=month, day=min(value.day, calendar.monthrange(year, month)[1]))


def collect_with_reporting_token(args, now):
    query = '''query($workspaceId:String!){workspace(workspaceId:$workspaceId){id name customer{billingPeriod{start end}}}
      projects(first:5000,includeDeleted:true,workspaceId:$workspaceId){edges{node{id name createdAt services{edges{node{id name createdAt}}}}}}}'''
    context = graphql_json(query, {'workspaceId': args.workspace})
    current = context['workspace']['customer']['billingPeriod']
    anchor = datetime.fromisoformat(current['start'].replace('Z', '+00:00'))
    projects = [x['node'] for x in context['projects']['edges']]
    project_by_id = {p['id']: p for p in projects}
    service_by_id = {s['node']['id']: s['node'] for p in projects for s in p['services']['edges']}
    rows, owners = [], {}
    raw_dir = Path(args.raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / 'resource-lifecycle.json').write_text(json.dumps(projects, indent=2), encoding='utf-8')
    usage_query = '''query($workspaceId:String!,$start:DateTime!,$end:DateTime!,$measurements:[MetricMeasurement!]!){
      usage(workspaceId:$workspaceId,startDate:$start,endDate:$end,measurements:$measurements,groupBy:[PROJECT_ID,SERVICE_ID],includeDeleted:true){measurement value tags{projectId serviceId}}}'''
    for selected in args.period or ['current', 'previous']:
        if selected == 'current':
            start = anchor
        elif selected == 'previous':
            start = shift_month(anchor, -1)
        else:
            year, month = map(int, selected.split('-'))
            start = anchor.replace(year=year, month=month, day=min(anchor.day, calendar.monthrange(year, month)[1]))
        end = shift_month(start, 1)
        if start > datetime.fromisoformat(now):
            raise ValueError('Future billing period rejected')
        period = {'start': start.isoformat(), 'end': end.isoformat()}
        result = graphql_json(usage_query, {'workspaceId': args.workspace, 'start': period['start'],
                    'end': period['end'], 'measurements': list(RATES)})
        costs = {}
        for value in result['usage']:
            service_id = value['tags'].get('serviceId')
            project_id = value['tags'].get('projectId')
            if not service_id or not project_id:
                raise RuntimeError('Native billable meter without resource identity requires review')
            identity = (project_id, service_id)
            costs[identity] = costs.get(identity, Decimal(0)) + Decimal(str(value['value'])) * RATES[value['measurement']]
        for (project_id, service_id), cost in costs.items():
            project, service = project_by_id[project_id], service_by_id[service_id]
            event_service = {**service, 'totalDollars': str(cost)}
            row = normalize_service(period, project, event_service, now, service['createdAt'])
            row['source_kind'] = 'provider_usage'
            if row['period_start'] >= row['period_end'] and cost == 0:
                continue
            rows.append(row)
            owners[service_id] = ownership(project, service, service['createdAt'])
        (raw_dir / f'{start:%Y-%m}.json').write_text(json.dumps({'snapshot_at': now, 'billing_period': period,
            'meters': result['usage'], 'rates_usd': {k: str(v) for k,v in RATES.items()},
            'rate_evidence': 'https://github.com/railwayapp/cli/blob/master/src/commands/usage.rs',
            'accounting_note': 'Native actual resource meters priced exactly as official Railway CLI. This is metered usage, not a reconciled invoice or cash settlement.'}, indent=2), encoding='utf-8')
    return rows, owners


def cli_json(args, executable=None):
    executable = executable or os.environ.get('RAILWAY_CLI') or shutil.which('railway.exe') or shutil.which('railway')
    if not executable:
        raise RuntimeError('Railway CLI is required; authenticate an account with workspace billing access')
    result = subprocess.run([executable, *args], capture_output=True, text=True, check=False)
    if result.returncode:
        raise RuntimeError(f'Railway read failed ({result.returncode}); provider output withheld')
    return json.loads(result.stdout)


def normalize_service(period, project, service, snapshot_at, created_at=None):
    resource_id = service['id']
    source_id = f"railway:{period['start']}:{period['end']}:{resource_id}"
    actual_start = max(period['start'], created_at) if created_at else period['start']
    actual_end = min(period['end'], snapshot_at)
    return {
        'vendor': 'railway', 'resource_id': resource_id, 'resource_name': service['name'],
        'period_start': actual_start, 'period_end': actual_end,
        'amount_usd': str(service['totalDollars']), 'currency': 'USD',
        'cost_kind': 'resource_usage', 'source_kind': 'provider_billing',
        'source_id': source_id,
        'evidence_url': f"https://railway.com/project/{project['id']}",
        'snapshot_at': snapshot_at,
    }


def ownership(project, service, created_at):
    owner, default_service = OWNERS.get(project['id'], ('unattributed', 'unattributed'))
    service_name = 'ops' if service['name'] == 'bibletimes-ops' else default_service
    return {'vendor': 'railway', 'resource_id': service['id'], 'project': owner,
            'service': service_name, 'environment': 'production',
            'valid_from': created_at, 'valid_to': None,
            'evidence_url': f"https://railway.com/project/{project['id']}"}


def creation_metadata():
    node = shutil.which('node')
    if not node:
        raise RuntimeError('Node.js is required for Railway resource lifecycle metadata')
    source = r'''
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
const token = process.env.RAILWAY_API_TOKEN || JSON.parse(fs.readFileSync(path.join(os.homedir(), '.railway/config.json'), 'utf8')).user.accessToken;
const ids = ['c775b566-4003-4e8c-b776-7e9195c0e85c','a95458c2-c47d-4a00-9f66-2a0001671ecd','6b2c3d13-23c6-4c76-9cd4-072600fb6a63','2f3bbf44-622c-4c7c-95cf-cbc1b1fab633'];
const query = 'query { ' + ids.map((id,i)=>`p${i}:project(id:"${id}"){id name createdAt services{edges{node{id name createdAt}}}}`).join(' ')+' }';
const r=await fetch('https://backboard.railway.com/graphql/v2',{method:'POST',headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/json'},body:JSON.stringify({query})});
if(!r.ok)throw new Error(`Railway metadata HTTP ${r.status}`);
const j=await r.json();if(j.errors)throw new Error('Railway metadata query failed');console.log(JSON.stringify(Object.values(j.data)));
'''
    result = subprocess.run([node, '--input-type=module', '-e', source], capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError('Railway lifecycle metadata read failed; provider output withheld')
    return json.loads(result.stdout)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--period', action='append', help='current, previous, or YYYY-MM; repeatable')
    parser.add_argument('--workspace', default='8022fa0a-1f9a-4e9c-8257-e04d60014f24')
    parser.add_argument('--output', required=True)
    parser.add_argument('--ownership-output', required=True)
    parser.add_argument('--raw-dir', required=True)
    parser.add_argument('--reporting-token-file')
    args = parser.parse_args()
    if args.reporting_token_file:
        os.environ['RAILWAY_API_TOKEN'] = Path(args.reporting_token_file).read_text(encoding='utf-8').strip()
    now = datetime.now(timezone.utc).isoformat()
    rows, owners = [], {}
    raw_dir = Path(args.raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    if os.environ.get('RAILWAY_API_TOKEN'):
        rows, owners = collect_with_reporting_token(args, now)
        for output, values in [(args.output, rows), (args.ownership_output, list(owners.values()))]:
            Path(output).parent.mkdir(parents=True, exist_ok=True)
            Path(output).write_text(''.join(json.dumps(row) + '\n' for row in values), encoding='utf-8')
        print(json.dumps({'provider': 'railway', 'resource_events': len(rows), 'ownership_rows': len(owners), 'auth': 'workspace_reporting_token', 'output': args.output}))
        return
    metadata = creation_metadata()
    created = {s['node']['id']: s['node']['createdAt'] for p in metadata for s in p['services']['edges']}
    (raw_dir / 'resource-lifecycle.json').write_text(json.dumps(metadata, indent=2), encoding='utf-8')
    for selected in args.period or ['current', 'previous']:
        common = ['--workspace', args.workspace, '--period', selected, '--json']
        listing = cli_json(['usage', 'projects', *common])
        summary = cli_json(['usage', *common])
        details = []
        for project in listing['projects']:
            detail = cli_json(['usage', 'projects', '--project', project['id'], *common])
            details.append(detail)
            for service in detail['services']:
                if service['id'] not in created:
                    raise RuntimeError('Unknown Railway service lifecycle; do not infer historical ownership')
                rows.append(normalize_service(detail['billingPeriod'], project, service, now, created[service['id']]))
                owners[service['id']] = ownership(project, service, created[service['id']])
        (raw_dir / f"{listing['period']}.json").write_text(json.dumps({
            'snapshot_at': now, 'summary': summary, 'projects': listing, 'service_details': details,
            'accounting_note': 'Native metered resource costs; plan minimums, included credits and cash settlement require invoice reconciliation. Do not add workspace/project totals to service rows.'
        }, indent=2), encoding='utf-8')
    for output, values in [(args.output, rows), (args.ownership_output, list(owners.values()))]:
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        Path(output).write_text(''.join(json.dumps(row) + '\n' for row in values), encoding='utf-8')
    print(json.dumps({'provider': 'railway', 'resource_events': len(rows), 'ownership_rows': len(owners), 'output': args.output}))


if __name__ == '__main__':
    main()
