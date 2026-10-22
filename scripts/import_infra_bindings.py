"""Import ownership metadata from service-key manifests; never copy secrets."""
import argparse
import csv
import json
import re
from datetime import date
from pathlib import Path

FIELDS = ['vendor', 'credential_id', 'project', 'service', 'environment', 'valid_from', 'valid_to']
PROJECT_FIELDS = ['vendor', 'provider_project_id', 'project', 'environment', 'valid_from', 'valid_to']


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('manifests', nargs='+', type=Path)
    p.add_argument('--valid-from', help='Explicit ownership date for manifests without created_at')
    p.add_argument('--output', type=Path, default=Path(__file__).parents[1] / 'seeds/infra_credential_bindings.csv')
    p.add_argument('--projects-output', type=Path, default=Path(__file__).parents[1] / 'seeds/infra_provider_project_bindings.csv')
    args = p.parse_args()
    with args.output.open(newline='') as stream:
        rows = list(csv.DictReader(stream))
    projects = []
    if args.projects_output.exists():
        with args.projects_output.open(newline='') as stream:
            projects = list(csv.DictReader(stream))
    for path in args.manifests:
        vendor = path.parent.parent.name
        if vendor not in {'openai', 'anthropic', 'openrouter'}:
            raise ValueError('Manifest must be in a provider/service-keys directory')
        for item in json.loads(path.read_text()):
            patterns = {'openrouter': r'[0-9a-f]{64}', 'openai': r'key_[A-Za-z0-9_-]+',
                        'anthropic': r'apikey_[A-Za-z0-9_-]+'}
            if not re.fullmatch(patterns[vendor], item['credential_id']):
                raise ValueError('Expected nonsecret provider key ID/hash; secret material is forbidden')
            valid_from = (item.get('created_at') or args.valid_from or '')[:10]
            date.fromisoformat(valid_from)
            provider_project = item.get('provider_project_id')
            if provider_project:
                matches = [r for r in projects if r['vendor'] == vendor and r['provider_project_id'] == provider_project]
                if any(r['project'] != item['project'] or r['environment'] != item['environment'] for r in matches):
                    raise ValueError('Provider project has conflicting ownership; review its mapping manually')
                if not matches:
                    projects.append({'vendor': vendor, 'provider_project_id': provider_project,
                                     'project': item['project'], 'environment': item['environment'],
                                     'valid_from': valid_from, 'valid_to': ''})
            if any(row['vendor'] == vendor and row['credential_id'] == item['credential_id'] for row in rows):
                continue
            if item['environment'] not in {'production', 'nonproduction'}:
                raise ValueError('Unsupported environment')
            rows.append({'vendor': vendor, 'credential_id': item['credential_id'],
                         'project': item['project'], 'service': item['service'],
                         'environment': item['environment'], 'valid_from': valid_from, 'valid_to': ''})
    with args.output.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    with args.projects_output.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=PROJECT_FIELDS)
        writer.writeheader()
        writer.writerows(projects)
    print(f'{len(rows)} nonsecret ownership bindings written')
    print(f'{len(projects)} nonsecret provider project/workspace bindings written')


if __name__ == '__main__':
    main()
