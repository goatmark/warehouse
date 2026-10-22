"""Refresh native hosting exports, ingest validated facts, then build reporting.

Cloud-only scheduling is in hosting-cost-sync.yml. Supplemental verified invoice
evidence stays in the raw warehouse; provider snapshots never truncate it.
"""
import argparse
from datetime import datetime,timedelta,timezone
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT=Path(__file__).parents[1]


def run(*args):
    subprocess.run([sys.executable,*map(str,args)],check=True,cwd=ROOT)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output-dir',type=Path,required=True)
    p.add_argument('--profiles-dir',required=True)
    args=p.parse_args();out=args.output_dir.resolve();out.mkdir(parents=True,exist_ok=True)
    scripts=ROOT/'scripts';collect=scripts/'hosting';loader=scripts/'hosting_resource_sync.py'
    now=datetime.now(timezone.utc)
    def load(vendor):
        run(loader,out/f'{vendor}.jsonl','--ownership-json',out/f'{vendor}.ownership.jsonl')
    run(collect/'vercel_costs.py','--from',(now-timedelta(days=40)).date().isoformat()+'T00:00:00Z',
        '--to',now.date().isoformat()+'T00:00:00Z','--output',out/'vercel.jsonl',
        '--ownership-output',out/'vercel.ownership.jsonl','--cache-dir',out/'raw/vercel')
    load('vercel')
    run(collect/'vercel_marketplace_gmail_collect.py','--output',out/'vercel-marketplace.jsonl',
        '--ownership-output',out/'vercel-marketplace.ownership.jsonl')
    load('vercel-marketplace')
    run(collect/'railway_collect.py','--period','current','--period','previous',
        '--output',out/'railway.jsonl','--ownership-output',out/'railway.ownership.jsonl','--raw-dir',out/'raw/railway')
    load('railway')
    run(collect/'digitalocean_collect.py','--from-period',(now-timedelta(days=100)).strftime('%Y-%m'),
        '--output',out/'digitalocean.jsonl','--ownership-output',out/'digitalocean.ownership.jsonl','--raw-dir',out/'raw/digitalocean')
    load('digitalocean')
    invoices=out/'supabase-invoices'
    run(collect/'supabase_gmail_fetch.py','--output-dir',invoices)
    run(collect/'supabase_invoice_collect.py','--invoice-dir',invoices,'--output',out/'supabase.jsonl',
        '--ownership-output',out/'supabase.ownership.jsonl','--raw-dir',out/'raw/supabase')
    load('supabase')
    run(collect/'gcp_billing_export_collect.py','--write-readiness','--output',out/'google_cloud.jsonl',
        '--ownership-output',out/'google_cloud.ownership.jsonl','--readiness-output',out/'google-cloud-readiness.json',
        '--raw-dir',out/'raw/google_cloud')
    load('google_cloud')
    subprocess.run(['dbt','build','--project-dir',str(ROOT),'--profiles-dir',args.profiles_dir,
        '--select','+rpt_infrastructure_cost_monthly','rpt_hosting_reconciliation_monthly','+rpt_hosting_source_coverage'],check=True,cwd=ROOT)
    print('Native hosting exporters, ownership, reconciliation and financial report completed')


if __name__=='__main__':main()
