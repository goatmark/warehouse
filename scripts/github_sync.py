"""
GitHub -> BigQuery sync
Pulls commits and repo metadata for goatmark repos.
Writes to data-warehouse-475122.github dataset.
Reads config from environment variables for CI use.
"""

import os, sys, time, requests
sys.stdout.reconfigure(encoding='utf-8')
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

def main():
    print('=== GitHub -> BigQuery sync ===')
    print(f'Repos: {REPOS}')
    client = bq_client()

    all_commits, all_repo_meta = [], []

    for repo in REPOS:
        print(f'-- {repo} --')
        all_commits.extend(fetch_commits(repo))
        all_repo_meta.append(fetch_repo_meta(repo))

    print('Writing to BigQuery...')
    upsert_table(client, 'commits_clean',      all_commits,   COMMITS_SCHEMA, 'WRITE_TRUNCATE')
    upsert_table(client, 'repositories_clean', all_repo_meta, REPOS_SCHEMA,   'WRITE_TRUNCATE')

    print(f'Done. commits={len(all_commits)} repos={len(all_repo_meta)}')

if __name__ == '__main__':
    main()
