"""Validate and ingest metadata-only hosting resource billing exports.

Each source_id identifies one vendor/resource/billing-period/line-item fact.
Re-exporting it supersedes its previous snapshot, never adds another charge.
"""
import argparse
import csv
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_EVEN
import hashlib
import json
import os
from pathlib import Path

TABLE = 'data-warehouse-475122.data_finance_business.infra_hosting_resource_events'
SCHEMA = [('record_id','STRING'),('vendor','STRING'),('billing_vendor','STRING'),('resource_id','STRING'),('resource_name','STRING'),('billing_service','STRING'),
          ('period_start','TIMESTAMP'),('period_end','TIMESTAMP'),('amount_usd','NUMERIC'),
          ('usage_cost_usd','NUMERIC'),
          ('raw_amount_usd','STRING'),('cost_kind','STRING'),('source_kind','STRING'),
          ('source_id','STRING'),('evidence_url','STRING'),('snapshot_at','TIMESTAMP'),('currency','STRING')]
VENDORS = {'railway','vercel','supabase','google_cloud','digitalocean'}


def timestamp(value):
    parsed = datetime.fromisoformat(str(value).replace('Z','+00:00'))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalize(row):
    vendor = row['vendor']
    if vendor not in VENDORS:
        raise ValueError('Unsupported hosting vendor')
    start,end = timestamp(row['period_start']),timestamp(row['period_end'])
    if start >= end or (end-start).days > 367:
        raise ValueError('Invalid billing interval')
    if row.get('currency','USD').upper() != 'USD':
        raise ValueError('Currency conversion needs explicit evidence; non-USD rejected')
    if row['cost_kind'] not in {'resource_usage','account_fee','invoice_adjustment'}:
        raise ValueError('Unsupported cost kind')
    if row['source_kind'] not in {'provider_billing','export','invoice','provider_usage'}:
        raise ValueError('Unsupported source kind')
    amount = Decimal(str(row['amount_usd']))
    if not amount.is_finite():
        raise ValueError('Nonfinite money')
    usage=Decimal(str(row['usage_cost_usd'])) if row.get('usage_cost_usd') is not None else None
    if usage is not None and not usage.is_finite():raise ValueError('Nonfinite usage cost')
    source_id = str(row['source_id'])
    resource_id = str(row['resource_id'])
    if not source_id or not resource_id or not row.get('evidence_url'):
        raise ValueError('Resource, source identity and evidence are required')
    # Native source identity fixes the billing line. Measured cutoff/amount can
    # change during the current period without creating another charge.
    identity = '|'.join([vendor,source_id,resource_id])
    return {'record_id':hashlib.sha256(identity.encode()).hexdigest(),'vendor':vendor,
            'billing_vendor':row.get('billing_vendor',vendor),
            'resource_id':resource_id,'resource_name':row.get('resource_name',resource_id),
            'billing_service':row.get('billing_service'),
            'period_start':start.isoformat(),'period_end':end.isoformat(),
            'amount_usd':str(amount.quantize(Decimal('0.000000001'),rounding=ROUND_HALF_EVEN)),
            'raw_amount_usd':str(amount),'cost_kind':row['cost_kind'],'source_kind':row['source_kind'],
            'usage_cost_usd':str(usage.quantize(Decimal('0.000000001'),rounding=ROUND_HALF_EVEN)) if usage is not None else None,
            'source_id':source_id,'evidence_url':row['evidence_url'],
            'snapshot_at':timestamp(row.get('snapshot_at',datetime.now(timezone.utc).isoformat())).isoformat(),
            'currency':'USD'}


def client_and_table():
    from google.cloud import bigquery
    os.environ.setdefault('GOOGLE_APPLICATION_CREDENTIALS','H:/My Drive/Infra/gcp-dwh/data-warehouse-475122-bf83c2643411.json')
    c=bigquery.Client()
    c.create_table(bigquery.Table(TABLE,schema=[bigquery.SchemaField(*f) for f in SCHEMA]),exists_ok=True)
    table=c.get_table(TABLE)
    existing={f.name for f in table.schema}
    missing=[bigquery.SchemaField(*f) for f in SCHEMA if f[0] not in existing]
    if missing:
        table.schema=list(table.schema)+missing;c.update_table(table,['schema'])
    return c


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('inputs',nargs='*',type=Path)
    p.add_argument('--ensure-table',action='store_true')
    p.add_argument('--dry-run',action='store_true')
    p.add_argument('--ownership-json',type=Path,action='append',default=[])
    p.add_argument('--replace-ownership',action='store_true',help='Explicitly correct reviewed resource ownership; never infer a transfer')
    a=p.parse_args()
    rows=[]
    for path in a.inputs:
        for line in path.read_text().splitlines():
            if line.strip(): rows.append(normalize(json.loads(line)))
    if len({r['record_id'] for r in rows}) != len(rows):
        raise ValueError('Duplicate source/resource/interval records in export')
    if a.ownership_json:
        seed=Path(__file__).parents[1]/'seeds/infra_hosting_resource_bindings.csv'
        fields=['vendor','resource_id','project','service','environment','valid_from','valid_to','evidence_url']
        with seed.open(newline='',encoding='utf-8') as stream:bindings=list(csv.DictReader(stream))
        for path in a.ownership_json:
            raw=path.read_text(encoding='utf-8').strip()
            ownership=json.loads(raw) if raw.startswith('[') else [json.loads(line) for line in raw.splitlines() if line.strip()]
            for row in ownership:
                binding={f:row.get(f,'') for f in fields}
                binding['valid_from']=timestamp(binding['valid_from']).date().isoformat()
                binding['valid_to']=timestamp(binding['valid_to']).date().isoformat() if binding['valid_to'] else ''
                if binding['vendor'] not in VENDORS or not all(binding[f] for f in fields if f!='valid_to'):
                    raise ValueError('Incomplete resource ownership/evidence')
                candidates=[b for b in bindings if b['vendor']==binding['vendor'] and b['resource_id']==binding['resource_id']]
                previous=next((b for b in candidates if not b['valid_to'] and not binding['valid_to']),None)
                if previous:
                    same_owner=all(previous[f]==binding[f] for f in ['project','service','environment'])
                    if not same_owner and not a.replace_ownership:raise ValueError('Ownership conflicts; explicit review/correction or dated transfer required')
                    if same_owner or a.replace_ownership:
                        first=min(previous['valid_from'],binding['valid_from'])
                        previous.update(binding);previous['valid_from']=first
                elif not candidates:bindings.append(binding)
                elif binding not in candidates:raise ValueError('Existing closed ownership intervals require explicit dated review')
        if not a.dry_run:
            with seed.open('w',newline='',encoding='utf-8') as stream:
                writer=csv.DictWriter(stream,fieldnames=fields);writer.writeheader();writer.writerows(bindings)
        print(f'Validated {len(bindings)} resource ownership bindings')
    if a.dry_run:
        print(f'Validated {len(rows)} hosting resource facts');return
    c=client_and_table()
    if rows:
        from google.cloud import bigquery
        config=bigquery.LoadJobConfig(schema=[bigquery.SchemaField(*f) for f in SCHEMA],write_disposition='WRITE_APPEND')
        c.load_table_from_json(rows,TABLE,job_config=config).result()
    print(f'Loaded {len(rows)} hosting resource facts')


if __name__=='__main__':main()
