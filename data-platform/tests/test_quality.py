import sys
import unittest
from pathlib import Path

PLATFORM_DIR = Path(__file__).resolve().parent.parent
if str(PLATFORM_DIR) not in sys.path:
    sys.path.insert(0, str(PLATFORM_DIR))

from quality.quality_checker import DataQualityChecker
from warehouse.db import db_manager


class DataQualityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db_manager.initialize_schema()
        cls.checker = DataQualityChecker()

    def test_completeness_and_validity_execution(self):
        c_passed, c_details = self.checker.run_completeness_checks()
        v_passed, v_details = self.checker.run_validity_checks()
        u_passed, u_details = self.checker.run_uniqueness_checks()

        self.assertTrue(isinstance(c_passed, bool))
        self.assertTrue(isinstance(v_passed, bool))
        self.assertTrue(isinstance(u_passed, bool))

    def test_full_quality_suite_run(self):
        result = self.checker.evaluate_quality_suite(stage_name="test_suite_execution")
        self.assertIn("metric_record", result)
        mr = result["metric_record"]
        self.assertIn("overall_status", mr)
        self.assertIn("raw_quality_rate_pct", mr)
        self.assertIn("warehouse_integrity_status", mr)
        self.assertIn("freshness_status", mr)


if __name__ == "__main__":
    unittest.main()
