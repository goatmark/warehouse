import importlib.util
from pathlib import Path
import unittest


def module(name):
    spec=importlib.util.spec_from_file_location(name,Path(__file__).parents[1]/'scripts'/f'{name}.py')
    result=importlib.util.module_from_spec(spec);spec.loader.exec_module(result);return result


hosting=module('hosting_resource_sync');operational=module('operational_usage_sync')


class MetadataAccountingTests(unittest.TestCase):
    def test_cross_month_period_preserves_timestamp_and_correction_identity(self):
        row={'vendor':'railway','resource_id':'r','period_start':'2026-09-02T23:12:51Z',
             'period_end':'2026-10-02T23:12:51Z','amount_usd':'2.43262445384','cost_kind':'resource_usage',
             'source_kind':'provider_usage','source_id':'invoice:cpu','evidence_url':'https://example.com/billing'}
        a=hosting.normalize(row);b=hosting.normalize({**row,'amount_usd':'3.00'})
        self.assertEqual(a['record_id'],b['record_id'])
        self.assertEqual(a['record_id'],hosting.normalize({**row,'period_end':'2026-10-02T00:00:00Z'})['record_id'])
        self.assertEqual(a['period_start'],'2026-09-02T23:12:51+00:00')
        self.assertEqual(a['amount_usd'],'2.432624454')
        with self.assertRaises(ValueError):hosting.normalize({**row,'currency':'EUR'})

    def test_operational_allowlist_drops_content_and_does_not_infer_historical_key(self):
        row={'kind':'call','at':'2026-10-18T06:48:05Z','model':'a','costUsd':0.25,
             'run':'historic','estimated':False,'promptTokens':5,'completionTokens':8,
             'prompt':'secret prompt','response':'private response','api_key':'secret key'}
        a=operational.normalize(row,Path('experiments/run/spend.jsonl'))
        b=operational.normalize(row,Path('experiments/copied/spend.jsonl'))
        self.assertEqual(a['record_id'],b['record_id'])
        self.assertEqual(a['project'],'prepsavvy')
        self.assertIsNone(a['credential_id'])
        self.assertFalse({'prompt','response','api_key'} & a.keys())
        self.assertIsNone(operational.normalize({'kind':'unit'},Path('ignored')))


if __name__=='__main__':unittest.main()
