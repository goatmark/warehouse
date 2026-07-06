"""
GitHub coding-activity -> BigQuery sync (v2)

Improves on scripts/github_sync.py:
  * Covers ALL owned, non-archived, non-fork, non-empty repos (enumerated live),
    not a hardcoded 3-repo list.
  * Captures per-commit line stats (additions, deletions, files_changed) via the
    per-commit detail endpoint GET /repos/{o}/{r}/commits/{sha}.
  * Incremental: only fetches detail for commit SHAs not already in BigQuery, so
    normal runs are cheap and full history is never re-pulled.
  * Writes to the NEW dataset `data_github` (tables `commits`, `repositories`).
    Never touches the legacy `github` dataset.
  * Rate-limit aware: stops gracefully and reports what is left to backfill.

Env:
  GH_PAT        GitHub token (repo read scope)
  GCP_SA_PATH   path to GCP service-account JSON
Optional:
  GH_OWNER      default 'goatmark'
  GH_SINCE      ISO start date for first backfill, default 2015-01-01
"""

import os, sys, time, requests
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery
from google.oauth2 import service_account

GITHUB_TOKEN = os.environ['GH_PAT']
GCP_SA_PATH  = os.environ['GCP_SA_PATH']
OWNER        = os.environ.get('GH_OWNER', 'goatmark')
PROJECT      = 'data-warehouse-475122'
DATASET      = 'data_github'
START        = os.environ.get('GH_SINCE', '2015-01-01T00:00:00Z')
RL_FLOOR     = 60   # stop fetching commit detail when core budget drops below this

GH_HEADERS = {
    'Authorization': f'Bearer {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github.v3+json',
    'X-GitHub-Api-Version': '2022-11-28',
}

_session = requests.Session()
_session.headers.update(GH_HEADERS)
_last_remaining = None


def gh_get(path, params=None):
    global _last_remaining
    url = path if path.startswith('http') else f'https://api.github.com{path}'
    last_exc = None
    for attempt in range(5):
        try:
            r = _session.get(url, params=params, timeout=30)
        except requests.exceptions.RequestException as e:
            last_exc = e
            time.sleep(2 * (attempt + 1))
            continue
        _last_remaining = r.headers.get('X-RateLimit-Remaining')
        # secondary rate limit / abuse
        if r.status_code in (403, 429) and 'rate limit' in r.text.lower():
            reset = int(r.headers.get('X-RateLimit-Reset', time.time() + 60))
            wait = max(5, min(120, reset - int(time.time())))
            print(f'   rate-limited, sleeping {wait}s')
            time.sleep(wait)
            continue
        if r.status_code >= 500:
            time.sleep(2 * (attempt + 1))
            continue
        r.raise_for_status()
        return r, r.json()
    raise last_exc if last_exc else RuntimeError(f'gh_get failed: {url}')


def gh_get_all(path, params=None):
    results = []
    url, p = path, params
    while url:
        r, data = gh_get(url, p)
        if not isinstance(data, list):
            return data
        results.extend(data)
        url = None
        for part in r.headers.get('Link', '').split(','):
            if 'rel="next"' in part:
                url = part.strip().split(';')[0].strip('<> ')
        p = None
        if url:
            time.sleep(0.03)
    return results


def rl_remaining():
    return int(_last_remaining) if _last_remaining is not None else 5000


# ---------------------------------------------------------------- BigQuery
def bq_client():
    creds = service_account.Credentials.from_service_account_file(GCP_SA_PATH)
    return bigquery.Client(project=PROJECT, credentials=creds)


COMMITS_SCHEMA = [
    bigquery.SchemaField('sha',           'STRING',    mode='REQUIRED'),
    bigquery.SchemaField('repo',          'STRING',    mode='REQUIRED'),
    bigquery.SchemaField('message',       'STRING'),
    bigquery.SchemaField('author_name',   'STRING'),
    bigquery.SchemaField('author_email',  'STRING'),
    bigquery.SchemaField('author_login',  'STRING'),
    bigquery.SchemaField('authored_at',   'TIMESTAMP'),
    bigquery.SchemaField('committed_at',  'TIMESTAMP'),
    bigquery.SchemaField('additions',     'INTEGER'),
    bigquery.SchemaField('deletions',     'INTEGER'),
    bigquery.SchemaField('files_changed', 'INTEGER'),
    bigquery.SchemaField('url',           'STRING'),
    bigquery.SchemaField('synced_at',     'TIMESTAMP'),
]

REPOS_SCHEMA = [
    bigquery.SchemaField('repo',           'STRING', mode='REQUIRED'),
    bigquery.SchemaField('description',    'STRING'),
    bigquery.SchemaField('language',       'STRING'),
    bigquery.SchemaField('stars',          'INTEGER'),
    bigquery.SchemaField('forks',          'INTEGER'),
    bigquery.SchemaField('open_issues',    'INTEGER'),
    bigquery.SchemaField('size_kb',        'INTEGER'),
    bigquery.SchemaField('is_private',     'BOOLEAN'),
    bigquery.SchemaField('is_fork',        'BOOLEAN'),
    bigquery.SchemaField('is_archived',    'BOOLEAN'),
    bigquery.SchemaField('default_branch', 'STRING'),
    bigquery.SchemaField('created_at',     'TIMESTAMP'),
    bigquery.SchemaField('updated_at',     'TIMESTAMP'),
    bigquery.SchemaField('pushed_at',      'TIMESTAMP'),
    bigquery.SchemaField('synced_at',      'TIMESTAMP'),
]


def ensure_dataset(client):
    ds_id = f'{PROJECT}.{DATASET}'
    try:
        client.get_dataset(ds_id)
        print(f'Dataset {ds_id} exists')
    except Exception:
        ds = bigquery.Dataset(ds_id)
        ds.location = 'US'
        client.create_dataset(ds, exists_ok=True)
        print(f'Created dataset {ds_id}')


def load_rows(client, table_id, rows, schema, write_mode):
    if not rows:
        print(f'  {table_id}: no rows ({write_mode})')
        return
    table_ref = f'{PROJECT}.{DATASET}.{table_id}'
    cfg = bigquery.LoadJobConfig(
        schema=schema, write_disposition=write_mode,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED)
    client.load_table_from_json(rows, table_ref, job_config=cfg).result()
    print(f'  {table_id}: wrote {len(rows)} rows ({write_mode})')


def existing_shas(client):
    """Map repo -> set(sha) already loaded, for incremental dedup."""
    out = {}
    try:
        q = f'SELECT repo, sha FROM `{PROJECT}.{DATASET}.commits`'
        for r in client.query(q).result():
            out.setdefault(r.repo, set()).add(r.sha)
    except Exception:
        pass  # table does not exist yet (first run)
    return out


# ---------------------------------------------------------------- GitHub fetch
def list_owned_repos():
    repos = gh_get_all('/user/repos',
                       params={'affiliation': 'owner', 'per_page': 100, 'sort': 'pushed'})
    keep = []
    for d in repos:
        if d.get('owner', {}).get('login', '').lower() != OWNER.lower():
            continue
        if d.get('archived') or d.get('fork') or d.get('size', 0) == 0:
            continue
        keep.append(d)
    return keep


def repo_meta_row(d, now):
    return {
        'repo': d['full_name'],
        'description': d.get('description') or '',
        'language': d.get('language') or '',
        'stars': d.get('stargazers_count', 0),
        'forks': d.get('forks_count', 0),
        'open_issues': d.get('open_issues_count', 0),
        'size_kb': d.get('size', 0),
        'is_private': d.get('private', False),
        'is_fork': d.get('fork', False),
        'is_archived': d.get('archived', False),
        'default_branch': d.get('default_branch', ''),
        'created_at': d.get('created_at'),
        'updated_at': d.get('updated_at'),
        'pushed_at': d.get('pushed_at'),
        'synced_at': now,
    }


def fetch_new_commit_rows(repo, have_shas, now, stop):
    """List commits, fetch per-commit stats only for SHAs we don't have yet."""
    listed = gh_get_all(f'/repos/{repo}/commits',
                        params={'since': START, 'per_page': 100})
    new_shas = [c['sha'] for c in listed if c['sha'] not in have_shas]
    rows, fetched = [], 0
    for sha in new_shas:
        if rl_remaining() <= RL_FLOOR:
            stop['hit'] = True
            break
        _, c = gh_get(f'/repos/{repo}/commits/{sha}')
        commit = c.get('commit', {})
        author = commit.get('author', {})
        committer = commit.get('committer', {})
        gh_auth = c.get('author') or {}
        stats = c.get('stats', {})
        rows.append({
            'sha': c['sha'],
            'repo': repo,
            'message': (commit.get('message', '') or '')[:2000],
            'author_name': author.get('name', ''),
            'author_email': author.get('email', ''),
            'author_login': gh_auth.get('login', ''),
            'authored_at': author.get('date'),
            'committed_at': committer.get('date'),
            'additions': stats.get('additions', 0),
            'deletions': stats.get('deletions', 0),
            'files_changed': len(c.get('files', []) or []),
            'url': c.get('html_url', ''),
            'synced_at': now,
        })
        fetched += 1
    return rows, len(listed), len(new_shas), fetched


def main():
    now = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    print('=== GitHub coding-activity -> BigQuery (data_github) ===')
    client = bq_client()
    ensure_dataset(client)

    have = existing_shas(client)
    repos = list_owned_repos()
    print(f'Owned repos to sync: {len(repos)} (core budget remaining {rl_remaining()})')

    all_commits, repo_rows = [], []
    stop = {'hit': False}
    incomplete = []

    for d in repos:
        repo = d['full_name']
        repo_rows.append(repo_meta_row(d, now))
        if stop['hit']:
            incomplete.append(repo + ' (not started)')
            continue
        rows, n_listed, n_new, n_fetched = fetch_new_commit_rows(
            repo, have.get(repo, set()), now, stop)
        all_commits.extend(rows)
        tag = ''
        if n_new != n_fetched:
            tag = f'  !! {n_new - n_fetched} remaining (rate limit)'
            incomplete.append(f'{repo} ({n_new - n_fetched} commits left)')
        print(f'  {repo}: listed={n_listed} new={n_new} fetched={n_fetched} '
              f'rl={rl_remaining()}{tag}')

    print('Writing BigQuery...')
    load_rows(client, 'commits', all_commits, COMMITS_SCHEMA, 'WRITE_APPEND')
    load_rows(client, 'repositories', repo_rows, REPOS_SCHEMA, 'WRITE_TRUNCATE')

    print(f'Done. new_commits={len(all_commits)} repos={len(repo_rows)} '
          f'rl_remaining={rl_remaining()}')
    if incomplete:
        print('INCOMPLETE (rate limited) — rerun to finish:')
        for x in incomplete:
            print('   -', x)


if __name__ == '__main__':
    main()
