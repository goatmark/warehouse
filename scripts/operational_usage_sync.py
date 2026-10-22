"""Import allowlisted factory meter metadata into a separate operational fact.

No prompts, responses, error bodies, API keys or arbitrary JSON are persisted.
Per-call meter costs never enter the provider financial totals.
"""
import argparse
from datetime import datetime,timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import tempfile
import urllib.request

ROOT=Path('C:/Users/mmajd/Projects/prepsavvy-factory').resolve()
CENTRAL=ROOT/'logs/ai-usage.jsonl'
CHECKPOINT=Path(__file__).parents[1]/'logs/operational-loaded-ids.json'
TABLE='data-warehouse-475122.data_finance_business.infra_operational_usage_events'
SCHEMA=[('record_id','STRING'),('at','TIMESTAMP'),('vendor','STRING'),('project','STRING'),
 ('service','STRING'),('environment','STRING'),('credential_id','STRING'),('run_id','STRING'),
 ('generator','STRING'),('stage','STRING'),('cell','STRING'),('model','STRING'),('provider','STRING'),
 ('cost_usd','NUMERIC'),('estimated','BOOL'),('prompt_tokens','INT64'),('completion_tokens','INT64'),
 ('reasoning_tokens','INT64'),('source_file','STRING'),('loaded_at','TIMESTAMP')]


def normalize(row,source):
    if row.get('kind','call')!='call':return None
    at=datetime.fromisoformat(row['at'].replace('Z','+00:00')).astimezone(timezone.utc).isoformat()
    # This source's registered ownership is explicit; historical rows lacking
    # environment/key/generator stay unknown rather than borrowing today's key.
    result={'at':at,'vendor':row.get('vendor','openrouter'),'project':'prepsavvy','service':'factory',
      'environment':row.get('environment') or 'unattributed','credential_id':row.get('credential_id'),
      'run_id':row.get('run_id') or row.get('run'),'generator':row.get('generator'),
      'stage':row.get('stage'),'cell':row.get('cell'),'model':row.get('model'),
      'provider':row.get('provider'),'cost_usd':str(Decimal(str(row['costUsd'])).quantize(Decimal('0.000000001'))),
      'estimated':bool(row.get('estimated',True)),'prompt_tokens':int(row.get('promptTokens',0)),
      'completion_tokens':int(row.get('completionTokens',0)),'reasoning_tokens':int(row.get('reasoningTokens',0))}
    if result['vendor']!='openrouter':raise ValueError('Factory source vendor changed; review registration')
    result['record_id']=hashlib.sha256(json.dumps(result,sort_keys=True).encode()).hexdigest()
    result['source_file']=str(source)
    result['loaded_at']=datetime.now(timezone.utc).isoformat()
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--ensure-table',action='store_true')
    p.add_argument('--dry-run',action='store_true')
    p.add_argument('--file',type=Path,action='append')
    p.add_argument('--drive',action='store_true',help='Read only the configured synchronized metadata file')
    args=p.parse_args()
    cloud_path=None
    if args.drive:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        info=json.loads(os.environ['OPERATIONAL_DRIVE_TOKEN_JSON']) if os.environ.get('OPERATIONAL_DRIVE_TOKEN_JSON') else json.loads(Path('H:/My Drive/Infra/google_mcp/docs_token.json').read_text())
        credentials=Credentials.from_authorized_user_info(info);credentials.refresh(Request())
        source=json.loads((Path(__file__).parents[1]/'config/operational_sources.json').read_text())['prepsavvy_factory']
        url='https://www.googleapis.com/drive/v3/files/'+source['drive_file_id']+'?alt=media'
        req=urllib.request.Request(url,headers={'Authorization':'Bearer '+credentials.token})
        descriptor,temporary=tempfile.mkstemp(suffix='.jsonl');os.close(descriptor);cloud_path=Path(temporary).resolve()
        with urllib.request.urlopen(req,timeout=120) as response,cloud_path.open('wb') as target:
            while block:=response.read(1024*1024):target.write(block)
        args.file=[cloud_path]
    paths=args.file if args.file else sorted((ROOT/'experiments').glob('**/*spend.jsonl'))+([CENTRAL] if CENTRAL.exists() else [])
    rows={}
    for path in paths:
        path=path.resolve()
        if path!=cloud_path and path!=CENTRAL and (not path.is_relative_to(ROOT/'experiments') or not path.name.endswith('spend.jsonl')):
            raise ValueError('Only allowlisted factory experiments/*spend.jsonl sources may be read')
        with path.open(encoding='utf-8') as stream:
            for line in stream:
                if not line.strip():continue
                source_name='drive:'+source['drive_file_id'] if path==cloud_path else path.relative_to(ROOT)
                row=normalize(json.loads(line),source_name)
                if row:rows[row['record_id']]=row
    if args.dry_run:
        print(f'Validated {len(rows)} unique metadata-only operational calls from {len(paths)} files');return
    from google.cloud import bigquery
    os.environ.setdefault('GOOGLE_APPLICATION_CREDENTIALS','H:/My Drive/Infra/gcp-dwh/data-warehouse-475122-bf83c2643411.json')
    c=bigquery.Client();schema=[bigquery.SchemaField(*f) for f in SCHEMA]
    c.create_table(bigquery.Table(TABLE,schema=schema),exists_ok=True)
    if CHECKPOINT.exists():known=set(json.loads(CHECKPOINT.read_text(encoding='utf-8')))
    else:known={row.record_id for row in c.query(f'SELECT DISTINCT record_id FROM `{TABLE}`').result()}
    pending=[row for identity,row in rows.items() if identity not in known]
    if pending:
        c.load_table_from_json(pending,TABLE,job_config=bigquery.LoadJobConfig(schema=schema,write_disposition='WRITE_APPEND')).result()
    CHECKPOINT.parent.mkdir(exist_ok=True)
    CHECKPOINT.write_text(json.dumps(sorted(known|rows.keys())),encoding='utf-8')
    print(f'Loaded {len(pending)} new operational calls; {len(rows)} scanned from {len(paths)} allowlisted files')
    if cloud_path:cloud_path.unlink()


if __name__=='__main__':main()
