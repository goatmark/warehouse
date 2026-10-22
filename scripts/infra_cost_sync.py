"""Read-only provider APIs -> append-only daily cost/usage snapshots.

Secrets are read from environment or H:/My Drive/Infra, never emitted. Billed
costs and token usage remain separate facts. Run --vendor openrouter --days 7.
"""
import argparse
import json
import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

TABLE = 'data-warehouse-475122.data_finance_business.infra_provider_events'
INFRA = Path(os.environ.get('INFRA_ROOT', 'H:/My Drive/Infra'))


def secret(vendor):
    env, filename = {
        'openai': ('OPENAI_ADMIN_KEY', 'openai/OPENAI_ADMIN_KEY.txt'),
        'anthropic': ('ANTHROPIC_ADMIN_KEY', 'anthropic/ADMIN_API_KEY.txt'),
        'openrouter': ('OPENROUTER_MANAGEMENT_KEY', 'openrouter/OPENROUTER_MANAGEMENT_KEY.txt'),
    }[vendor]
    value = os.environ.get(env)
    if value:
        return value.strip()
    return (INFRA / filename).read_text().strip()


def request(url, headers, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = Request(url, data=data, headers={**headers, 'Content-Type': 'application/json'})
    # HTTP errors propagate without printing response bodies or headers.
    with urlopen(req, timeout=90) as response:
        return json.load(response)


def pages(url, params, headers):
    results = []
    seen = set()
    while True:
        data = request(url + '?' + urlencode(params, doseq=True), headers)
        results.extend(data.get('data', []))
        if not data.get('has_more'):
            return results
        cursor = data.get('next_page')
        if not cursor or cursor in seen:
            raise ValueError('Missing or repeated provider pagination cursor')
        seen.add(cursor)
        params = {**params, 'page': cursor}


def day_of(bucket):
    value = bucket.get('starting_at') or bucket.get('start_time_iso')
    if value:
        return value[:10]
    return datetime.fromtimestamp(bucket['start_time'], timezone.utc).date().isoformat()


def event(day, basis, result, **values):
    return {'event_date': day, 'cost_basis': basis,
            'credential_id': result.get('api_key_id'),
            'provider_project_id': result.get('project_id') or result.get('workspace_id'),
            'model': result.get('model'), 'line_item': result.get('line_item') or result.get('description'),
            'prompt_tokens': 0, 'completion_tokens': 0, 'cached_tokens': 0,
            'requests': None, 'cost_usd': None, 'is_marker': False, **values}


def openai_rows(key, start, end):
    headers = {'Authorization': 'Bearer ' + key}
    params = {'start_time': int(start.timestamp()), 'end_time': int(end.timestamp()),
              'bucket_width': '1d', 'limit': 31}
    usage = pages('https://api.openai.com/v1/organization/usage/completions',
                  {**params, 'group_by[]': ['model', 'project_id', 'api_key_id']}, headers)
    usage.extend(pages('https://api.openai.com/v1/organization/usage/embeddings',
                  {**params, 'group_by[]': ['model', 'project_id', 'api_key_id']}, headers))
    costs = pages('https://api.openai.com/v1/organization/costs',
                  {**params, 'group_by[]': ['project_id', 'api_key_id', 'line_item']}, headers)
    rows = []
    for bucket in usage:
        for r in bucket.get('results', []):
            rows.append(event(day_of(bucket), 'provider_usage', r,
                prompt_tokens=int(r.get('input_tokens', 0)), completion_tokens=int(r.get('output_tokens', 0)),
                cached_tokens=int(r.get('input_cached_tokens', 0)), requests=int(r.get('num_model_requests', 0))))
    for bucket in costs:
        for r in bucket.get('results', []):
            amount = r.get('amount') or {}
            if amount.get('currency', 'usd').lower() != 'usd':
                raise ValueError('Non-USD OpenAI cost requires currency conversion')
            rows.append(event(day_of(bucket), 'billed_cost', r, cost_usd=str(Decimal(str(amount.get('value', 0))))))
    return rows, ['provider_usage', 'billed_cost']


def anthropic_rows(key, start, end):
    headers = {'x-api-key': key, 'anthropic-version': '2023-06-01'}
    params = {'starting_at': start.isoformat(), 'ending_at': end.isoformat(), 'bucket_width': '1d', 'limit': 31}
    usage = pages('https://api.anthropic.com/v1/organizations/usage_report/messages',
                  {**params, 'group_by[]': ['model', 'workspace_id', 'api_key_id']}, headers)
    costs = pages('https://api.anthropic.com/v1/organizations/cost_report',
                  {**params, 'group_by[]': ['workspace_id', 'description']}, headers)
    rows = []
    for bucket in usage:
        for r in bucket.get('results', []):
            cache = r.get('cache_creation') or {}
            prompt = int(r.get('uncached_input_tokens', 0)) + int(r.get('cache_read_input_tokens', 0)) + sum(int(v) for v in cache.values())
            rows.append(event(day_of(bucket), 'provider_usage', r, prompt_tokens=prompt,
                completion_tokens=int(r.get('output_tokens', 0)), cached_tokens=int(r.get('cache_read_input_tokens', 0))))
    for bucket in costs:
        for r in bucket.get('results', []):
            rows.append(event(day_of(bucket), 'billed_cost', r, cost_usd=str(Decimal(str(r.get('amount', 0))) / 100)))
    return rows, ['provider_usage', 'billed_cost']


def openrouter_rows(key, start, end):
    headers = {'Authorization': 'Bearer ' + key}
    meta = request('https://openrouter.ai/api/v1/analytics/meta', headers)['data']
    if 'api_key_id' not in {d['name'] for d in meta['dimensions']}:
        raise ValueError('OpenRouter key attribution dimension unavailable')
    keys = []
    offset = 0
    while True:
        batch = request('https://openrouter.ai/api/v1/keys?offset=' + str(offset), headers).get('data', [])
        keys.extend(batch)
        if len(batch) < 100:
            break
        offset += len(batch)
    by_name = {}
    for item in keys:
        by_name.setdefault(item.get('name'), []).append(item['hash'])

    def fetch(a, b):
        body = {'metrics': ['request_count', 'tokens_prompt', 'tokens_completion', 'total_usage', 'cached_tokens'],
                'dimensions': ['model', 'api_key_id'], 'granularity': 'day', 'limit': 10000,
                'time_range': {'start': a.strftime('%Y-%m-%dT%H:%M:%SZ'), 'end': b.strftime('%Y-%m-%dT%H:%M:%SZ')}}
        data = request('https://openrouter.ai/api/v1/analytics/query', headers, body)['data']
        if data.get('warnings'):
            raise ValueError('OpenRouter analytics warnings require review')
        if data.get('metadata', {}).get('truncated'):
            if (b-a).total_seconds() <= 86400:
                raise ValueError('One-day OpenRouter result truncated; no partial snapshot loaded')
            midpoint = a + timedelta(days=max(1, (b-a).days // 2))
            return fetch(a, midpoint) + fetch(midpoint, b)
        return data.get('data', [])

    rows = []
    for r in fetch(start, end):
        label = r.get('api_key_id')
        candidates = by_name.get(label, [])
        identifier = candidates[0] if len(candidates) == 1 else 'unresolved-label:' + str(label)
        rows.append(event(r['date__day'][:10], 'provider_usage', {'model': r.get('model'), 'api_key_id': identifier},
            cost_usd=str(Decimal(str(r.get('total_usage', 0)))), prompt_tokens=int(r.get('tokens_prompt', 0)),
            completion_tokens=int(r.get('tokens_completion', 0)), cached_tokens=int(r.get('cached_tokens', 0)),
            requests=int(r.get('request_count', 0))))
    return rows, ['provider_usage']


def snapshot(vendor, start, end):
    rows, bases = globals()[vendor + '_rows'](secret(vendor), start, end)
    now = datetime.now(timezone.utc).isoformat()
    # Mark every fetched day/basis, including empty buckets, so deleted/corrected
    # rows are superseded by a complete snapshot, never mixed across loads.
    d = start.date()
    while d < end.date():
        for basis in bases:
            rows.append(event(d.isoformat(), basis, {}, is_marker=True))
        d += timedelta(days=1)
    for row in rows:
        row.update(vendor=vendor, snapshot_at=now)
    return rows


def load(rows):
    from google.cloud import bigquery
    os.environ.setdefault('GOOGLE_APPLICATION_CREDENTIALS', str(INFRA / 'gcp-dwh/data-warehouse-475122-bf83c2643411.json'))
    fields = [('event_date', 'DATE'), ('snapshot_at', 'TIMESTAMP'), ('vendor', 'STRING'), ('cost_basis', 'STRING'),
              ('provider_project_id', 'STRING'), ('credential_id', 'STRING'), ('model', 'STRING'), ('line_item', 'STRING'),
              ('cost_usd', 'NUMERIC'), ('prompt_tokens', 'INT64'), ('completion_tokens', 'INT64'), ('cached_tokens', 'INT64'),
              ('requests', 'INT64'), ('is_marker', 'BOOL')]
    config = bigquery.LoadJobConfig(schema=[bigquery.SchemaField(*f) for f in fields], write_disposition='WRITE_APPEND')
    bigquery.Client().load_table_from_json(rows, TABLE, job_config=config).result()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--vendor', choices=['openai', 'anthropic', 'openrouter'], required=True)
    parser.add_argument('--days', type=int, default=7)
    parser.add_argument('--start')
    parser.add_argument('--end')
    args = parser.parse_args()
    end = datetime.combine(date.fromisoformat(args.end) if args.end else datetime.now(timezone.utc).date(), datetime.min.time(), timezone.utc)
    start = datetime.combine(date.fromisoformat(args.start), datetime.min.time(), timezone.utc) if args.start else end - timedelta(days=args.days)
    if start >= end:
        raise ValueError('start must precede exclusive end')
    while start < end:
        chunk_end = min(start + timedelta(days=31), end)
        rows = snapshot(args.vendor, start, chunk_end)
        load(rows)
        print(f'{args.vendor}: loaded {len(rows)} rows, {start.date()} through {chunk_end.date()} exclusive')
        start = chunk_end


if __name__ == '__main__':
    main()
