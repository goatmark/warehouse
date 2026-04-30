"""
GitHub -> BigQuery sync
Pulls commits, contributor stats, and repo metadata for goatmark repos.
Writes to data-warehouse-475122.github dataset.
Reads config from environment variables for CI use.
"""

import os, sys, json, time, requests
sys.stdout.reconfigure(encoding='utf-8')
from datetime import datetime, timezone
from google.cloud import bigquery
from google.oauth2 import service_account

GITHUB_TOKEN = os.environ['GH_PAT']
REPOS        = ['goatmark/website', 'goatmark/warehouse', 'goatmark/bibletimes']
GCP_SA_PATH  = os.environ['GCP_SA_PATH']
PROJECT      = 'data-warehouse-475122'
DATASET      = 'github'
START        = '2023-01-01T00:00:00Z'

GH_HEADERS = {
    'Authorization': f'Bearer {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github.v3+json',
    'X-GitHub-Api-Version': '2022-11-28',
}

def gh_get_all(path, params=None):
    url = f'https://api.github.com{path}'
    results = []
    while url:
        r = requests.get(url, headers=GH_HEADERS, params=params)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            results.extend(data)
        else:
            return data
        next_url = None
        for part in r.headers.get('Link', '').split(','):
            if 'rel="next"' in part:
                next_url = part.strip().split(';')[0].strip('<> ')
        url = next_url
        params = None
        if next_url:
            time.sleep(0.05)
    return results

def gh_get(path, params=None):
    r = requests.get(f'https://api.github.com{path}', headers=GH_HEADERS, params=params)
    r.raise_for_status()
    return r.json()

def bq_client():
    creds = service_account.Credentials.from_service_account_file(GCP_SA_PATH)
    return bigquery.Client(project=PROJECT, credentials=creds)

def upsert_table(client, table_id, rows, schema, write_mode):
    if not rows:
        print(f'  No rows for {table_id}, skipping')
        return
    table_ref = f'{PROJECT}.{DATASET}.{table_id}'
    job_cfg = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=write_mode,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    job = client.load_table_from_json(rows, table_ref, job_config=job_cfg)
    job.result()
    print(f'  {table_id}: wrote {len(rows)} rows ({write_mode})')

COMMITS_SCHEMA = [
    bigquery.SchemaField('sha',           'STRING', mode='REQUIRED'),
    bigquery.SchemaField('repo',          'STRING', mode='REQUIRED'),
    bigquery.SchemaField('message',       'STRING'),
    bigquery.SchemaField('author_name',   'STRING'),
    bigquery.SchemaField('author_email',  'STRING'),
    bigquery.SchemaField('author_login',  'STRING'),
    bigquery.SchemaField('committed_at',  'TIMESTAMP'),
    bigquery.SchemaField('url',           'STRING'),
]

REPOS_SCHEMA = [
    bigquery.SchemaField('repo',           'STRING', mode='REQUIRED'),
    bigquery.SchemaField('description',    'STRING'),
    bigquery.SchemaField('language',       'STRING'),
    bigquery.SchemaField('stars',          'INTEGER'),
    bigquery.SchemaField('forks',          'INTEGER'),
    bigquery.SchemaField('open_issues',    'INTEGER'),
    bigquery.SchemaField('is_private',     'BOOLEAN'),
    bigquery.SchemaField('default_branch', 'STRING'),
    bigquery.SchemaField('created_at',     'TIMESTAMP'),
    bigquery.SchemaField('pushed_at',      'TIMESTAMP'),
]

STATS_SCHEMA = [
    bigquery.SchemaField('repo',            'STRING', mode='REQUIRED'),
    bigquery.SchemaField('author_login',    'STRING', mode='REQUIRED'),
    bigquery.SchemaField('total_commits',   'INTEGER'),
    bigquery.SchemaField('total_additions', 'INTEGER'),
    bigquery.SchemaField('total_deletions', 'INTEGER'),
    bigquery.SchemaField('synced_at',       'TIMESTAMP'),
]

WEEKLY_STATS_SCHEMA = [
    bigquery.SchemaField('repo',         'STRING', mode='REQUIRED'),
    bigquery.SchemaField('author_login', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('week_start',   'DATE',   mode='REQUIRED'),
    bigquery.SchemaField('commits',      'INTEGER'),
    bigquery.SchemaField('additions',    'INTEGER'),
    bigquery.SchemaField('deletions',    'INTEGER'),
]

def fetch_commits(repo):
    print(f'  Fetching commits for {repo}...')
    raw = gh_get_all(f'/repos/{repo}/commits', params={'since': START, 'per_page': 100})
    rows = []
    for c in raw:
        commit  = c.get('commit', {})
        author  = commit.get('author', {})
        gh_auth = c.get('author') or {}
        rows.append({
            'sha':          c['sha'],
            'repo':         repo,
            'message':      commit.get('message', '')[:2000],
            'author_name':  author.get('name', ''),
            'author_email': author.get('email', ''),
            'author_login': gh_auth.get('login', ''),
            'committed_at': author.get('date', ''),
            'url':          c.get('html_url', ''),
        })
    print(f'  {repo}: {len(rows)} commits')
    return rows

def fetch_repo_meta(repo):
    print(f'  Fetching metadata for {repo}...')
    d = gh_get(f'/repos/{repo}')
    return {
        'repo':           d['full_name'],
        'description':    d.get('description') or '',
        'language':       d.get('language') or '',
        'stars':          d.get('stargazers_count', 0),
        'forks':          d.get('forks_count', 0),
        'open_issues':    d.get('open_issues_count', 0),
        'is_private':     d.get('private', False),
        'default_branch': d.get('default_branch', ''),
        'created_at':     d.get('created_at', ''),
        'pushed_at':      d.get('pushed_at', ''),
    }

def fetch_contributor_stats(repo, max_attempts=20, wait_sec=30):
    print(f'  Fetching contributor stats for {repo}...')
    now = datetime.now(timezone.utc).isoformat()
    url = f'https://api.github.com/repos/{repo}/stats/contributors'

    raw = []
    for attempt in range(max_attempts):
        r = requests.get(url, headers=GH_HEADERS)
        print(f'  Attempt {attempt+1}: status={r.status_code}')
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and data:
                raw = data
                break
        if attempt < max_attempts - 1:
            time.sleep(wait_sec)

    if not raw:
        print(f'  Stats not available after {max_attempts} attempts — skipping')
        return [], []

    summary_rows, weekly_rows = [], []
    for contributor in raw:
        login = (contributor.get('author') or {}).get('login', 'unknown')
        weeks = contributor.get('weeks', [])
        summary_rows.append({
            'repo':            repo,
            'author_login':    login,
            'total_commits':   sum(w.get('c', 0) for w in weeks),
            'total_additions': sum(w.get('a', 0) for w in weeks),
            'total_deletions': sum(w.get('d', 0) for w in weeks),
            'synced_at':       now,
        })
        for w in weeks:
            if w.get('c', 0) or w.get('a', 0) or w.get('d', 0):
                week_date = datetime.fromtimestamp(w['w'], tz=timezone.utc).strftime('%Y-%m-%d')
                weekly_rows.append({
                    'repo':         repo,
                    'author_login': login,
                    'week_start':   week_date,
                    'commits':      w.get('c', 0),
                    'additions':    w.get('a', 0),
                    'deletions':    w.get('d', 0),
                })

    print(f'  {repo}: {len(summary_rows)} contributors, {len(weekly_rows)} active weeks')
    return summary_rows, weekly_rows

def main():
    print('=== GitHub -> BigQuery sync ===')
    print(f'Repos: {REPOS}')
    client = bq_client()

    all_commits, all_repo_meta, all_stats_summary, all_stats_weekly = [], [], [], []

    for repo in REPOS:
        print(f'-- {repo} --')
        repo_commits = fetch_commits(repo)
        all_commits.extend(repo_commits)
        all_repo_meta.append(fetch_repo_meta(repo))
        summary, weekly = fetch_contributor_stats(repo)
        all_stats_summary.extend(summary)
        all_stats_weekly.extend(weekly)

    print('Writing to BigQuery...')
    upsert_table(client, 'commits_clean',            all_commits,       COMMITS_SCHEMA,      'WRITE_TRUNCATE')
    upsert_table(client, 'repositories_clean',       all_repo_meta,     REPOS_SCHEMA,        'WRITE_TRUNCATE')
    upsert_table(client, 'contributor_stats',        all_stats_summary, STATS_SCHEMA,        'WRITE_TRUNCATE')
    upsert_table(client, 'contributor_stats_weekly', all_stats_weekly,  WEEKLY_STATS_SCHEMA, 'WRITE_TRUNCATE')

    print(f'Done. commits={len(all_commits)} repos={len(all_repo_meta)} contributors={len(all_stats_summary)}')

if __name__ == '__main__':
    main()
