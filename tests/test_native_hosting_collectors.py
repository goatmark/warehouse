import sys
import unittest
from pathlib import Path
from decimal import Decimal

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts' / 'hosting'))
from supabase_invoice_collect import parse_invoice, normalize, period_dates
from railway_collect import normalize_service
from digitalocean_collect import normalize as normalize_do


class SupabaseInvoiceTests(unittest.TestCase):
    def test_nested_compute_discount_is_not_applied_twice(self):
        text = '''Invoice date Oct 21, 2026
Description Quantity Rate Amount
Compute Hours
Sep 21 – Oct 20, 2026
$4.83
Compute Hours (Small)
Sep 21 – Oct 20, 2026
$14.83
caqlsrowqmztusqvkhno 720 $0.0206 $14.83
Discount ($10.00 across 18 prices) -$10.00
Pro Plan
Oct 21 – Nov 20, 2026
1 $25.00 $25.00
Memo:
Subtotal $29.83
Chicago (15%) $4.47
Amount due $34.30'''
        invoice = parse_invoice(text, 'XALGDR-90001')
        self.assertEqual(invoice['charges'][0]['net'], '4.83')
        self.assertEqual(sum(Decimal(x['amount_usd']) for x in normalize(invoice, '/source.pdf', '2026-10-22T00:00:00Z')), Decimal('34.30'))
        with self.assertRaisesRegex(ValueError, 'does not reconcile'):
            parse_invoice(text.replace('Subtotal $29.83', 'Subtotal $39.83'), 'XALGDR-90001')

    def test_prior_balance_is_settlement_not_new_expense(self):
        text = '''Invoice date Mar 21, 2026
Description Quantity Rate Amount
Pro Plan
Mar 21 – Apr 20, 2026
1 $25.00 $25.00
Memo:
Subtotal $25.00
Applied balance $0.01
Amount due $25.01'''
        invoice = parse_invoice(text, 'XALGDR-90002')
        self.assertEqual(invoice['incurred_cost'], '25.00')
        self.assertEqual(invoice['amount_due'], '25.01')
        self.assertEqual(sum(Decimal(x['amount_usd']) for x in normalize(invoice, '/source.pdf', '2026-10-22T00:00:00Z')), Decimal('25.00'))

    def test_native_date_ranges_handle_same_month_and_year_boundary(self):
        self.assertEqual(period_dates('Jun 11 – 20, 2026')[:2], ('2026-06-11', '2026-06-21'))
        self.assertEqual(period_dates('Dec 21 – Jan 20, 2026')[:2], ('2025-12-21', '2026-01-21'))

    def test_paid_receipt_reconciles_same_invoice_without_rebooking_prior_balance(self):
        text = '''Receipt date Mar 21, 2026
Description Quantity Rate Amount
Pro Plan
Mar 21 \ufffd Apr 20, 2026
1 $25.00 $25.00
Memo:
Subtotal $25.00
Applied balance $0.01
Amount paid $25.01'''
        invoice = parse_invoice(text, 'XALGDR-90003')
        self.assertEqual(invoice['settlement_label'], 'amount_paid')
        self.assertEqual(invoice['incurred_cost'], '25.00')
        self.assertEqual(invoice['amount_due'], '25.01')
        with self.assertRaisesRegex(ValueError, 'do not reconcile'):
            parse_invoice(text.replace('Amount paid $25.01', 'Amount paid $25.02'), 'XALGDR-90003')


class NativeResourceTests(unittest.TestCase):
    def test_current_meter_does_not_allocate_cost_into_future_or_before_creation(self):
        period = {'start': '2026-10-02T00:00:00Z', 'end': '2026-11-02T00:00:00Z'}
        project, service = {'id': 'project'}, {'id': 'service', 'name': 'service', 'totalDollars': '1.25'}
        first = normalize_service(period, project, service, '2026-10-22T00:00:00Z', '2026-10-10T00:00:00Z')
        second = normalize_service(period, project, service, '2026-10-23T00:00:00Z', '2026-10-10T00:00:00Z')
        self.assertEqual(first['period_start'], '2026-10-10T00:00:00Z')
        self.assertEqual(first['period_end'], '2026-10-22T00:00:00Z')
        self.assertEqual(first['source_id'], second['source_id'])

    def test_digitalocean_preserves_actual_resource_hours_and_unique_invoice_identity(self):
        item = {'resource_id': '599463143', 'product': 'Droplets', 'description': 'research', 'amount': '4.30',
                'start_time': '2026-09-10T21:53:21Z', 'end_time': '2026-10-01T00:00:00Z'}
        first = normalize_do({'invoice_uuid': 'invoice-one'}, item, 0, '2026-10-22T00:00:00Z')
        second = normalize_do({'invoice_uuid': 'invoice-two'}, item, 0, '2026-10-22T00:00:00Z')
        self.assertEqual(first['period_start'], item['start_time'])
        self.assertEqual(first['amount_usd'], '4.30')
        self.assertNotEqual(first['source_id'], second['source_id'])


if __name__ == '__main__':
    unittest.main()
