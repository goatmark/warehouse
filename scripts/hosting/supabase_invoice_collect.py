"""Extract native Supabase PDF charges, retaining discounts and reconciling totals."""
import argparse
import hashlib
import json
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

CATEGORIES = ['Disk Size GB-Hrs', 'Compute Hours', 'Cached Egress GB', 'Egress GB',
              'Function Invocations', 'IPv4 Hours', 'Monthly Active Users',
              'Storage Size GB-Hrs', 'Storage Image Transformations', 'Pro Plan']
MONEY = r'\$([\d,]+\.\d{2})(?!\d)'
MONTH = r'Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec'
PERIOD = re.compile(rf'({MONTH})\s+(\d{{1,2}})\s*[–-]\s*(?:({MONTH})\s+)?(\d{{1,2}}),\s*(\d{{4}})')


def amount(value):
    return Decimal(value.replace(',', ''))


def period_dates(text):
    match = PERIOD.search(text)
    if not match:
        raise ValueError('Native invoice line billing period missing')
    sm, sd, em, ed, year = match.groups()
    em = em or sm
    year = int(year)
    start = datetime.strptime(f'{sm} {sd} {year}', '%b %d %Y')
    end = datetime.strptime(f'{em} {ed} {year}', '%b %d %Y')
    if start > end:
        start = start.replace(year=year - 1)
    return start.date().isoformat(), (end + timedelta(days=1)).date().isoformat(), match.group(0)


def parse_invoice(text, invoice_id):
    # Gmail attaches both invoices and paid receipts for the same native invoice.
    # Receipts retain the original lines but use Amount paid / Receipt date.
    text = ''.join(c for c in text if unicodedata.category(c) != 'Cf').replace('\x00', '-').replace('\ufffd', '-')
    start = text.find('Description Quantity')
    end = text.find('Memo:', start)
    if start < 0 or end < 0:
        raise ValueError('Invoice table bounds missing')
    table = text[start:end]
    pattern = re.compile(r'^(' + '|'.join(re.escape(x) for x in CATEGORIES) + r')(?!\s*\()(?:\s|$)', re.M)
    headings = list(pattern.finditer(table))
    charges = []
    for index, heading in enumerate(headings):
        category = heading.group(1)
        segment = table[heading.start():headings[index + 1].start() if index + 1 < len(headings) else len(table)]
        # Parent compute/disk totals sometimes already include their nested-price discount.
        child = re.search(r'^' + re.escape(category) + r'\s*\(', segment, re.M)
        gross_region = segment[child.start():] if child else segment
        gross_match = re.search(MONEY, gross_region)
        if not gross_match:
            raise ValueError(f'{invoice_id}: amount missing for {category}')
        gross = amount(gross_match.group(1))
        discounts = sum((amount(x) for x in re.findall(r'Discount[^\n]*-\$([\d,]+\.\d{2})(?!\d)', segment)), Decimal(0))
        period_start, period_end, native_period = period_dates(segment)
        refs = sorted(set(re.findall(r'\b[a-z]{20}\b', segment)))
        if len(refs) > 1:
            raise ValueError(f'{invoice_id}: multi-resource group requires explicit allocation')
        charges.append({'category': category, 'gross': str(gross), 'discount': str(discounts),
                        'net': str(gross - discounts), 'resource_ref': refs[0] if refs else None,
                        'period_start': period_start, 'period_end': period_end, 'native_period': native_period})
    subtotal = amount(re.search(r'Subtotal\s+' + MONEY, text).group(1))
    settlement = re.search(r'Amount (due|paid)\s+' + MONEY, text)
    if not settlement:
        raise ValueError(f'{invoice_id}: native invoice settlement total missing')
    total = amount(settlement.group(2))
    taxes = sum((amount(x) for x in re.findall(r'Chicago\s*\([^)]*\)\s+' + MONEY, text)), Decimal(0))
    applied_match = re.search(r'Applied balance\s+(-?)' + MONEY, text)
    applied = amount(applied_match.group(2)) * (-1 if applied_match.group(1) else 1) if applied_match else Decimal(0)
    computed = sum((Decimal(x['net']) for x in charges), Decimal(0))
    if computed != subtotal:
        raise ValueError(f'{invoice_id}: line subtotal {computed} does not reconcile to {subtotal}')
    if subtotal + taxes + applied != total:
        raise ValueError(f'{invoice_id}: subtotal, tax and applied balance do not reconcile')
    issue = re.search(r'(?:Date of issue|Invoice date|Receipt date)\s+([A-Z][a-z]{2} \d{1,2}, \d{4})', text)
    if not issue:
        raise ValueError(f'{invoice_id}: issue date missing')
    issued = datetime.strptime(issue.group(1), '%b %d, %Y').date()
    return {'invoice_id': invoice_id, 'issue_date': issued.isoformat(), 'charges': charges,
            'subtotal': str(subtotal), 'tax': str(taxes), 'applied_prior_balance': str(applied),
            'amount_due': str(total), 'incurred_cost': str(subtotal + taxes), 'currency': 'USD',
            'settlement_label': 'amount_' + settlement.group(1),
            'reconciled': True,
            'period_note': 'PDF periods have date granularity; exclusive end is displayed end date plus one day. Calendar allocation is declared proration, not measured daily usage.'}


def normalize(invoice, evidence, snapshot_at):
    rows = []
    prefix = invoice['invoice_id'].split('-')[0]
    org_resource = 'organization:uvmnylbxtwlxhqfsurko' if prefix == 'XALGDR' else f'account:{prefix}'
    for line in invoice['charges']:
        resource = line['resource_ref'] or org_resource
        rows.append({'vendor': 'supabase', 'resource_id': resource,
            'resource_name': line['category'], 'period_start': line['period_start'], 'period_end': line['period_end'],
            'amount_usd': line['net'], 'currency': 'USD',
            'cost_kind': 'account_fee' if line['category'] == 'Pro Plan' else 'resource_usage',
            'source_kind': 'invoice', 'source_id': f"supabase:{invoice['invoice_id']}:{line['category']}",
            'evidence_url': evidence, 'snapshot_at': snapshot_at})
    if Decimal(invoice['tax']):
        start = datetime.fromisoformat(invoice['issue_date'])
        rows.append({'vendor': 'supabase', 'resource_id': org_resource, 'resource_name': 'Invoice tax',
            'period_start': invoice['issue_date'], 'period_end': (start + timedelta(days=1)).date().isoformat(),
            'amount_usd': invoice['tax'], 'currency': 'USD', 'cost_kind': 'invoice_adjustment',
            'source_kind': 'invoice', 'source_id': f"supabase:{invoice['invoice_id']}:tax",
            'evidence_url': evidence, 'snapshot_at': snapshot_at})
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--invoice-dir', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--ownership-output', required=True)
    parser.add_argument('--raw-dir', required=True)
    args = parser.parse_args()
    from pypdf import PdfReader
    now = datetime.now(timezone.utc).isoformat()
    evidence_file=Path(args.invoice_dir)/'gmail-invoice-evidence.json'
    evidence={r['invoice_id']:r['evidence_url'] for r in json.loads(evidence_file.read_text(encoding='utf-8'))} if evidence_file.exists() else {}
    rows, summaries = [], []
    raw_dir = Path(args.raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    for file in sorted(Path(args.invoice_dir).glob('Invoice-*.pdf')):
        text = '\n'.join(page.extract_text() for page in PdfReader(file).pages)
        invoice_id = file.stem.removeprefix('Invoice-')
        invoice = parse_invoice(text, invoice_id)
        invoice['source_sha256'] = hashlib.sha256(file.read_bytes()).hexdigest()
        invoice['evidence_file'] = str(file.resolve()).replace('\\', '/')
        rows.extend(normalize(invoice, evidence.get(invoice_id,invoice['evidence_file']), now))
        summaries.append(invoice)
        (raw_dir / f'{invoice_id}.json').write_text(json.dumps(invoice, indent=2), encoding='utf-8')
    known = [
        {'vendor': 'supabase', 'resource_id': 'caqlsrowqmztusqvkhno', 'project': 'bibletimes', 'service': 'database', 'environment': 'production', 'valid_from': '2026-01-21', 'valid_to': None,
         'evidence_url': 'https://supabase.com/dashboard/project/caqlsrowqmztusqvkhno'},
        {'vendor': 'supabase', 'resource_id': 'organization:uvmnylbxtwlxhqfsurko', 'project': 'bibletimes', 'service': 'platform', 'environment': 'production', 'valid_from': '2026-01-21', 'valid_to': None,
         'evidence_url': 'https://supabase.com/dashboard/org/uvmnylbxtwlxhqfsurko'},
    ]
    for output, values in [(args.output, rows), (args.ownership_output, known)]:
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        Path(output).write_text(''.join(json.dumps(row) + '\n' for row in values), encoding='utf-8')
    print(json.dumps({'provider': 'supabase', 'invoices': len(summaries), 'resource_events': len(rows),
        'incurred_cost_usd': str(sum((Decimal(x['incurred_cost']) for x in summaries), Decimal(0))),
        'amount_due_usd': str(sum((Decimal(x['amount_due']) for x in summaries), Decimal(0))),
        'all_invoices_reconciled': all(x['reconciled'] for x in summaries)}))


if __name__ == '__main__':
    main()
