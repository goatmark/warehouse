import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch
from datetime import datetime, timezone

spec = importlib.util.spec_from_file_location('sync', Path(__file__).parents[1] / 'scripts/infra_cost_sync.py')
sync = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sync)


class AccountingTests(unittest.TestCase):
    def test_openai_cost_is_not_copied_over_models_or_cached_tokens(self):
        usage = [{'start_time': 1792022400, 'results': [
            {'model': 'a', 'input_tokens': 100, 'input_cached_tokens': 40, 'output_tokens': 10},
            {'model': 'b', 'input_tokens': 200, 'output_tokens': 20}]}]
        costs = [{'start_time': 1792022400, 'results': [{'amount': {'value': '2.30', 'currency': 'usd'}}]}]
        with patch.object(sync, 'pages', side_effect=[usage, [], costs]):
            rows, _ = sync.openai_rows('test', datetime.now(timezone.utc), datetime.now(timezone.utc))
        self.assertEqual(sum(float(r['cost_usd'] or 0) for r in rows), 2.30)
        self.assertEqual(sum(r['prompt_tokens'] + r['completion_tokens'] for r in rows), 330)
        self.assertEqual(len([r for r in rows if r['cost_usd'] is not None]), 1)

    def test_anthropic_cents_and_separate_cache_components(self):
        usage = [{'starting_at': '2026-10-15T00:00:00Z', 'results': [
            {'uncached_input_tokens': 20, 'cache_read_input_tokens': 30,
             'cache_creation': {'ephemeral_1h_input_tokens': 40}, 'output_tokens': 10}]}]
        costs = [{'starting_at': '2026-10-15T00:00:00Z', 'results': [{'amount': '123.45'}]}]
        with patch.object(sync, 'pages', side_effect=[usage, costs]):
            rows, _ = sync.anthropic_rows('test', datetime.now(timezone.utc), datetime.now(timezone.utc))
        self.assertEqual(rows[0]['prompt_tokens'] + rows[0]['completion_tokens'], 100)
        self.assertEqual(rows[1]['cost_usd'], '1.2345')

    def test_repeated_page_cursor_refuses_incomplete_export(self):
        with patch.object(sync, 'request', return_value={'data': [], 'has_more': True, 'next_page': 'again'}):
            with self.assertRaises(ValueError):
                sync.pages('https://example.invalid', {}, {})

    def test_empty_days_have_snapshot_markers(self):
        a = datetime(2026, 10, 15, tzinfo=timezone.utc)
        b = datetime(2026, 10, 17, tzinfo=timezone.utc)
        with patch.object(sync, 'secret', return_value='test'), patch.object(sync, 'openai_rows', return_value=([], ['provider_usage', 'billed_cost'])):
            rows = sync.snapshot('openai', a, b)
        self.assertEqual(len(rows), 4)
        self.assertTrue(all(r['is_marker'] for r in rows))
        self.assertEqual(len({r['snapshot_at'] for r in rows}), 1)


if __name__ == '__main__':
    unittest.main()
