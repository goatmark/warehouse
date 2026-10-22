import json
from decimal import Decimal
import unittest
from vercel_costs import normalize_focus, owner


def focus(**updates):
    row = {'ChargePeriodStart': '2026-10-01T07:00:00Z', 'ChargePeriodEnd': '2026-10-02T07:00:00Z',
        'ChargeCategory': 'Usage', 'BilledCost': '2.5', 'EffectiveCost': '5', 'BillingCurrency': 'USD',
        'ServiceName': 'Function Duration', 'SkuId': 'sku-duration', 'ConsumedQuantity': 10,
        'ConsumedUnit': 'seconds', 'PricingCategory': 'Committed', 'PricingUnit': 'USD',
        'PricingCurrency': 'USD', 'Tags': {'ProjectId': 'prj-test', 'ProjectName': 'prepsavvy'}}
    row.update(updates)
    return json.dumps(row)


class VercelCostsTest(unittest.TestCase):
    def normalize(self, lines):
        return normalize_focus(lines, 'team-test', 'test-team', '2026-10-22T00:00:00Z')

    def test_conserves_billed_and_keeps_covered_usage_separate(self):
        rows, bindings = self.normalize([focus(), focus(BilledCost='0', EffectiveCost='3')])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['amount_usd'], Decimal('2.5'))
        self.assertEqual(rows[0]['usage_cost_usd'], Decimal('8'))
        self.assertEqual(bindings[0]['project'], 'prepsavvy')
        self.assertEqual(bindings[0]['environment'], 'mixed')

    def test_amount_correction_does_not_create_new_identity(self):
        before, _ = self.normalize([focus()])
        after, _ = self.normalize([focus(BilledCost='9.99')])
        self.assertEqual(before[0]['source_id'], after[0]['source_id'])
        other_region, _ = self.normalize([focus(RegionId='fra1')])
        self.assertNotEqual(before[0]['source_id'], other_region[0]['source_id'])

    def test_platform_fee_and_unlinked_blob_are_not_invented_venture_costs(self):
        rows, bindings = self.normalize([focus(ServiceName='Pro', Tags={}),
                                        focus(ServiceName='Blob Storage', SkuId='blob', Tags={})])
        self.assertEqual(rows[0]['cost_kind'], 'account_fee')
        self.assertEqual(bindings[0]['project'], 'shared')
        self.assertEqual(bindings[0]['service'], 'shared-platform-fee')
        self.assertEqual(bindings[1]['service'], 'unattributed')

    def test_zero_billed_covered_usage_is_retained(self):
        rows, _ = self.normalize([focus(BilledCost='0', EffectiveCost='1.25')])
        self.assertEqual(rows[0]['amount_usd'], 0)
        self.assertEqual(rows[0]['usage_cost_usd'], Decimal('1.25'))

    def test_rejects_unsupported_currency(self):
        with self.assertRaises(ValueError):
            self.normalize([focus(BillingCurrency='EUR')])

    def test_preserves_unrelated_projects(self):
        self.assertEqual(owner('ad-lab', 'Function Duration'), ('ad-lab', 'serving'))


if __name__ == '__main__':
    unittest.main()
