import sys
import unittest
from datetime import datetime
from pathlib import Path

PLATFORM_DIR = Path(__file__).resolve().parent.parent
if str(PLATFORM_DIR) not in sys.path:
    sys.path.insert(0, str(PLATFORM_DIR))

from transformations.transformer import (
    DataTransformer,
    generate_date_key,
    generate_warning_code,
)


class TransformationTests(unittest.TestCase):
    def setUp(self):
        self.transformer = DataTransformer()

    def test_date_key_and_dim_date_generation(self):
        dt = datetime(2026, 9, 22, 12, 0, 0)
        date_key = generate_date_key(dt)
        self.assertEqual(date_key, 20260922)

        dim_records = self.transformer.build_dim_date_records({dt})
        self.assertEqual(len(dim_records), 1)
        self.assertEqual(dim_records[0]["date_key"], 20260922)
        self.assertEqual(dim_records[0]["day_name"], "Tuesday")
        self.assertEqual(dim_records[0]["quarter"], 3)
        self.assertEqual(dim_records[0]["year"], 2026)

    def test_verification_fact_and_warning_link_transformation(self):
        raw_valid = [
            {
                "event_id": "v_trans_01",
                "timestamp": "2026-09-22T08:30:00+00:00",
                "input_type": "text_only",
                "risk_score": 75,
                "risk_level": "high",
                "warnings": [
                    "The advert requests or mentions a recruitment fee.",
                    "WhatsApp only.",
                ],
                "suspicious_domains": [],
                "is_shortened_url": False,
                "processing_time_ms": 32.5,
            }
        ]

        facts, links, dim_warnings, dates = self.transformer.transform_verifications(raw_valid)

        self.assertEqual(len(facts), 1)
        self.assertEqual(facts[0]["verification_id"], "v_trans_01")
        self.assertEqual(facts[0]["date_key"], 20260922)
        self.assertEqual(facts[0]["risk_level"], "high")

        self.assertEqual(len(links), 2)
        # dim_warnings now contains the full standardized catalogue pre-populated from WARNING_CATALOGUE
        self.assertGreaterEqual(len(dim_warnings), 2)
        # Link codes should be valid warning codes (catalogue codes, not MD5 hashes)
        self.assertTrue(len(links[0]["warning_code"]) > 0)


if __name__ == "__main__":
    unittest.main()
