import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

SCRIPT = Path(__file__).parents[1] / 'scripts' / 'operational_usage_sync.py'
spec = importlib.util.spec_from_file_location('operational_usage_sync', SCRIPT)
operational = importlib.util.module_from_spec(spec)
spec.loader.exec_module(operational)

GOOD = json.dumps({'kind': 'call', 'at': '2026-11-04T10:00:00Z', 'costUsd': 0.01, 'model': 'm'})


def run_dry(lines):
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder).resolve()
        central = root / 'ai-usage.jsonl'
        central.write_text('\n'.join(lines) + '\n', encoding='utf-8')
        with mock.patch.object(operational, 'ROOT', root), mock.patch.object(operational, 'CENTRAL', central), \
                mock.patch.object(sys, 'argv', ['sync', '--dry-run', '--file', str(central)]):
            operational.main()


class MalformedLines(unittest.TestCase):
    def test_a_few_torn_lines_are_skipped(self):
        run_dry([GOOD, 'd":"call"}', '}', GOOD])

    def test_a_damaged_file_still_fails(self):
        with self.assertRaises(ValueError):
            run_dry([GOOD] + ['d":"call"}'] * (operational.MALFORMED_FLOOR + 1))


if __name__ == '__main__':
    unittest.main()
