from datetime import datetime, timedelta, timezone
import importlib.util
from pathlib import Path
import unittest

SCRIPT = Path(__file__).parents[1] / 'scripts' / 'operational_usage_sync.py'
spec = importlib.util.spec_from_file_location('operational_usage_sync', SCRIPT)
operational = importlib.util.module_from_spec(spec)
spec.loader.exec_module(operational)

NEWEST = datetime(2026, 11, 10, 23, 19, 42, tzinfo=timezone.utc)


class MirrorGap(unittest.TestCase):
    def test_key_idle_since_newest_call_is_current(self):
        self.assertIsNone(operational.mirror_gap(NEWEST, NEWEST))
        self.assertIsNone(operational.mirror_gap(NEWEST, None))

    def test_drive_sync_lag_is_tolerated(self):
        self.assertIsNone(operational.mirror_gap(NEWEST, NEWEST + timedelta(hours=operational.MIRROR_LAG_HOURS)))

    def test_key_used_after_the_mirror_stopped_fails(self):
        # 2026-11-12 12:51 UTC: the factory key's last use while the mirror ended 11-10 23:19.
        gap = operational.mirror_gap(NEWEST, datetime(2026, 11, 12, 12, 51, 22, tzinfo=timezone.utc))
        self.assertAlmostEqual(gap, 37.5, places=1)

    def test_empty_mirror_with_key_use_fails(self):
        self.assertEqual(operational.mirror_gap(None, NEWEST), float('inf'))


if __name__ == '__main__':
    unittest.main()
