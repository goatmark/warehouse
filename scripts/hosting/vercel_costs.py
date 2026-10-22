"""Read Vercel FOCUS billed costs, preserving project/SKU and shared fees.

Official contract: https://vercel.com/docs/rest-api/billing/list-focus-billing-charges
Only GET requests are made. BilledCost is expense evidence; EffectiveCost is
separate usage-cost evidence and must never be added to BilledCost.
"""
import argparse
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import subprocess
import shutil
import urllib.error
import urllib.parse
import urllib.request

BASE = Path('H:/My Drive/Infra/ai-cost-attribution/hosting-resources')
TEAMS = {
    'goatmarks-projects': ('team_eqHjIkG5TAohedASe9hAxiTh', 'VERCEL_GOATMARK_TOKEN',
        Path('C:/Users/mmajd/AppData/Roaming/xdg.data/com.vercel.cli/auth.json')),
    'prep-savvy': ('team_H0eSiHH2kOn2EyTrMo6GxSqq', 'VERCEL_PREPSAVVY_TOKEN',
        Path('C:/Users/mmajd/AppData/Roaming/vercel-prepsavvy-owner/auth.json')),
}


def owner(project_name, service_name, team_id=None):
    if project_name == 'ad-lab' and team_id == 'team_H0eSiHH2kOn2EyTrMo6GxSqq':
        return ('prepsavvy', 'ad-lab')
    if not project_name:
        return ('shared', 'shared-platform-fee' if service_name == 'Pro' else 'unattributed')
    if project_name in {'bibletimes', 'bibletimes-intel', 'bt-report'}:
        return ('bibletimes', 'serving' if project_name == 'bibletimes' else 'analytics')
    if project_name in {'prepsavvy', 'prepsavvy-ads-sync'}:
        return ('prepsavvy', 'serving' if project_name == 'prepsavvy' else 'ops')
    if project_name in {'website-template', 'website'}:
        return ('website', 'serving')
    # Preserve distinct named projects. Never reassign unrelated apps to a venture.
    return (project_name, 'serving')


def normalize_focus(lines, team_id, slug, snapshot_at):
    grouped = {}
    raw_total = Decimal(0)
    for line in lines:
        if not line.strip():
            continue
        row = json.loads(line, parse_float=Decimal)
        if row.get('BillingCurrency') != 'USD':
            raise ValueError('Non-USD billing requires evidenced conversion')
        tags = row.get('Tags') or {}
        project_id, project_name = tags.get('ProjectId'), tags.get('ProjectName')
        billed = Decimal(str(row['BilledCost']))
        effective = Decimal(str(row.get('EffectiveCost', 0)))
        quantity = Decimal(str(row.get('ConsumedQuantity', 0)))
        if not all(x.is_finite() for x in (billed, effective, quantity)):
            raise ValueError('Nonfinite provider metric')
        raw_total += billed
        if not (billed or effective or quantity):
            continue
        start, end = row['ChargePeriodStart'], row['ChargePeriodEnd']
        if datetime.fromisoformat(start.replace('Z', '+00:00')) >= datetime.fromisoformat(end.replace('Z', '+00:00')):
            raise ValueError('Invalid provider interval')
        sku = row['SkuId']
        resource = f'{team_id}/{project_id or "account"}/{sku}'
        # Monetary values and mutable display names do not participate in identity.
        identity = [team_id, start, end, sku, row['ChargeCategory'], project_id,
                    row.get('RegionId'), row.get('PricingCategory'), row.get('ConsumedUnit'),
                    row.get('PricingUnit'), row.get('PricingCurrency'), row['BillingCurrency'],
                    {k: v for k, v in tags.items() if k != 'ProjectName'}]
        source_id = 'vercel-focus:' + hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        if source_id not in grouped:
            kind = 'account_fee' if row['ServiceName'] == 'Pro' and not project_id else 'resource_usage'
            if row['ChargeCategory'] not in {'Usage', 'Purchase'}:
                kind = 'invoice_adjustment'
            grouped[source_id] = {
                'vendor': 'vercel', 'resource_id': resource,
                'resource_name': f'{project_name or slug + " shared account"} / {row["ServiceName"]}',
                'period_start': start, 'period_end': end, 'amount_usd': Decimal(0),
                'usage_cost_usd': Decimal(0), 'cost_kind': kind, 'source_kind': 'export',
                'source_id': source_id, 'snapshot_at': snapshot_at, 'currency': 'USD',
                'evidence_url': f'https://api.vercel.com/v1/billing/charges?teamId={team_id}',
                'billing_service': row['ServiceName'], 'sku_id': sku, 'team_id': team_id,
                'provider_project_id': project_id, 'provider_project_name': project_name,
                'consumed_quantity': Decimal(0), 'consumed_unit': row.get('ConsumedUnit'),
                'pricing_category': row.get('PricingCategory'),
                'region_id': row.get('RegionId'),
            }
        item = grouped[source_id]
        item['amount_usd'] += billed
        item['usage_cost_usd'] += effective
        item['consumed_quantity'] += quantity
    rows = list(grouped.values())
    assert sum((x['amount_usd'] for x in rows), Decimal(0)) == raw_total
    ownership = {}
    for row in rows:
        project, service = owner(row['provider_project_name'], row['billing_service'], team_id)
        rid = row['resource_id']
        interval_start = row['period_start'][:10]
        if rid not in ownership:
            ownership[rid] = {'vendor': 'vercel', 'resource_id': rid, 'project': project,
                'service': service, 'environment': 'mixed', 'valid_from': interval_start,
                'valid_to': None, 'evidence_url': row['evidence_url']}
        ownership[rid]['valid_from'] = min(ownership[rid]['valid_from'], interval_start)
    return rows, list(ownership.values())


def fetch_focus(team_id, token, start, end, target):
    query = urllib.parse.urlencode({'teamId': team_id, 'from': start, 'to': end})
    request = urllib.request.Request('https://api.vercel.com/v1/billing/charges?' + query,
                                    headers={'Authorization': 'Bearer ' + token})
    temporary = target.with_suffix(target.suffix + '.tmp')
    try:
        with urllib.request.urlopen(request, timeout=120) as response, temporary.open('wb') as out:
            while block := response.read(1024 * 1024):
                out.write(block)
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)


def credential(env_name, auth_path):
    if os.environ.get(env_name):
        return os.environ[env_name]
    auth_env = env_name.replace('_TOKEN', '_AUTH_FILE')
    auth_path = Path(os.environ.get(auth_env, str(auth_path)))
    # Let the CLI refresh its existing OAuth session before reading its token.
    # No new access grant is requested, and no token is printed or put in argv.
    cli = Path(os.environ.get('VERCEL_CLI_ENTRY', 'C:/Users/mmajd/AppData/Roaming/npm/node_modules/vercel/dist/vc.js'))
    command = ['node', str(cli)] if cli.is_file() else [shutil.which('vercel') or 'vercel']
    refreshed = subprocess.run([*command, 'whoami', '--format', 'json',
        '--global-config', str(auth_path.parent), '--non-interactive'],
        capture_output=True, text=True, encoding='utf8', timeout=45)
    if refreshed.returncode:
        raise RuntimeError(f'Vercel billing session needs renewal for {env_name}')
    return json.loads(auth_path.read_text())['token']


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--from', dest='start', default=date.today().replace(day=1).isoformat() + 'T00:00:00Z')
    p.add_argument('--to', dest='end', default=datetime.now(timezone.utc).date().isoformat() + 'T00:00:00Z')
    p.add_argument('--output', type=Path, default=BASE / 'vercel.jsonl')
    p.add_argument('--ownership-output', type=Path, default=BASE / 'vercel.ownership.json')
    p.add_argument('--cache-dir', type=Path, default=BASE)
    p.add_argument('--use-cache', action='store_true')
    p.add_argument('--cache-year', default=str(date.today().year))
    args = p.parse_args()
    args.cache_dir.mkdir(parents=True, exist_ok=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.ownership_output.parent.mkdir(parents=True, exist_ok=True)
    snapshot = datetime.now(timezone.utc).isoformat()
    all_rows, bindings = [], []
    for slug, (team, env_name, auth_path) in TEAMS.items():
        cache = args.cache_dir / f'{slug}-focus-{args.cache_year}.raw.jsonl' if args.use_cache else args.cache_dir / f'{slug}-focus-current.raw.jsonl'
        if not args.use_cache:
            token = credential(env_name, auth_path)
            fetch_focus(team, token, args.start, args.end, cache)
        with cache.open(encoding='utf-8-sig') as source:
            rows, owned = normalize_focus(source, team, slug, snapshot)
        all_rows.extend(rows)
        bindings.extend(owned)
        print(f'{slug}: {len(rows)} normalized billing facts, {len(owned)} resource bindings')
    if len({r['source_id'] for r in all_rows}) != len(all_rows):
        raise ValueError('Duplicate normalized billing identity')
    with args.output.open('w', encoding='utf8') as out:
        for row in all_rows:
            out.write(json.dumps(row, default=str) + '\n')
    args.ownership_output.write_text(json.dumps(bindings, indent=2), encoding='utf8')
    print(f'Wrote {len(all_rows)} billed facts and {len(bindings)} ownership rows')


if __name__ == '__main__':
    main()
