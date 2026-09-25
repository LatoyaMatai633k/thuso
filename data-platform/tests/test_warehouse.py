import sys
import unittest
from pathlib import Path

PLATFORM_DIR = Path(__file__).resolve().parent.parent
if str(PLATFORM_DIR) not in sys.path:
    sys.path.insert(0, str(PLATFORM_DIR))

from warehouse.db import db_manager
from warehouse.loader import WarehouseLoader


class WarehouseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db_manager.initialize_schema()
        cls.loader = WarehouseLoader()

    def test_schema_tables_exist(self):
        df_tables = db_manager.query("SELECT name FROM sqlite_master WHERE type='table'") if db_manager.db_type == "sqlite" else db_manager.query("SELECT table_name as name FROM information_schema.tables WHERE table_schema='public'")
        table_names = set(df_tables["name"].tolist())

        self.assertIn("dim_date", table_names)
        self.assertIn("dim_risk_category", table_names)
        self.assertIn("dim_input_type", table_names)
        self.assertIn("fact_verifications", table_names)
        self.assertIn("fact_safe_journeys", table_names)
        self.assertIn("data_quality_metrics", table_names)

    def test_idempotent_dimension_load(self):
        records = [
            {"input_type_code": "test_type_1", "display_name": "Test Type", "description": "Desc"}
        ]
        inserted_first = self.loader.load_dimension("dim_input_type", records, "input_type_code")
        inserted_second = self.loader.load_dimension("dim_input_type", records, "input_type_code")

        self.assertEqual(inserted_second, 0)  # Idempotent: 0 duplicates inserted


if __name__ == "__main__":
    unittest.main()
