"""Collect finalized DigitalOcean invoice resource lines; previews are excluded."""
import argparse
import hashlib
import json
import os
import re
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

BASE = 'https://api.digitalocean.com'


def get_json(path, token):
    url = path if path.startswith(BASE + '/') else BASE + path
    request = urllib.request.Request(url, headers={'Authorization': 'Bearer ' + token})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)


def pages(path, token, field):
    while path:
        response = get_json(path, token)
        yield from response.get(field, [])
        path = response.get('links', {}).get('pages', {}).get('next')


def normalize(invoice, item, index, snapshot_at):
    resource = str(item.get('resource_uuid') or item.get('resource_id') or 'account')
    raw_id = json.dumps([invoice['invoice_uuid'], resource, item.get('product'), item.get('description'), item.get('start_time'), item.get('end_time'), index])
    identity = hashlib.sha256(raw_id.encode()).hexdigest()
    return {'vendor': 'digitalocean', 'resource_id': resource,
            'resource_name': item.get('description') or item.get('product') or 'Account adjustment',
            'period_start': item['start_time'], 'period_end': item['end_time'],
            'amount_usd': str(item['amount']), 'currency': 'USD',
            'cost_kind': 'resource_usage' if resource != 'account' else 'invoice_adjustment',
            'source_kind': 'invoice', 'source_id': f"digitalocean:{identity}",
            'evidence_url': f"https://cloud.digitalocean.com/account/billing/invoices/{invoice['invoice_uuid']}",
            'snapshot_at': snapshot_at}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--token-file', default=os.environ.get('DIGITALOCEAN_TOKEN_FILE'))
    parser.add_argument('--from-period', default='2026-07')
    parser.add_argument('--output', required=True)
    parser.add_argument('--ownership-output', required=True)
    parser.add_argument('--raw-dir', required=True)
    args = parser.parse_args()
    token = os.environ.get('DIGITALOCEAN_TOKEN')
    if not token and args.token_file:
        raw = Path(args.token_file).read_text(encoding='utf-8').strip()
        match = re.search(r'dop_v1_[A-Za-z0-9]+', raw)
        token = match.group(0) if match else raw
    if not token:
        raise RuntimeError('DIGITALOCEAN_TOKEN or --token-file is required')
    now = datetime.now(timezone.utc).isoformat()
    rows, owners, invoices = [], {}, []
    raw_dir = Path(args.raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    for invoice in pages('/v2/customers/my/invoices?per_page=100', token, 'invoices'):
        if invoice.get('invoice_period', '') < args.from_period:
            continue
        items = list(pages(f"/v2/customers/my/invoices/{invoice['invoice_uuid']}?per_page=100", token, 'invoice_items'))
        invoices.append({'invoice': invoice, 'items': items})
        for index, item in enumerate(items):
            row = normalize(invoice, item, index, now)
            rows.append(row)
            if row['resource_id'] == '599463143':
                owners[row['resource_id']] = {'vendor': 'digitalocean', 'resource_id': row['resource_id'],
                    'project': 'kalshi', 'service': 'research', 'environment': 'production',
                    'valid_from': '2026-09-10T21:52:48Z', 'valid_to': None,
                    'evidence_url': 'https://cloud.digitalocean.com/droplets/599463143'}
    (raw_dir / 'finalized-invoices.json').write_text(json.dumps({'snapshot_at': now, 'invoices': invoices,
        'accounting_note': 'Only finalized invoice resource lines. Payments, prior balances and invoice previews are not additional resource costs.'}, indent=2), encoding='utf-8')
    for output, values in [(args.output, rows), (args.ownership_output, list(owners.values()))]:
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        Path(output).write_text(''.join(json.dumps(row) + '\n' for row in values), encoding='utf-8')
    print(json.dumps({'provider': 'digitalocean', 'resource_events': len(rows), 'ownership_rows': len(owners), 'output': args.output}))


if __name__ == '__main__':
    main()
