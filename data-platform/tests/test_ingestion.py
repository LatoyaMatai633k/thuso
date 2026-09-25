import json
import sys
import unittest
from pathlib import Path

PLATFORM_DIR = Path(__file__).resolve().parent.parent
if str(PLATFORM_DIR) not in sys.path:
    sys.path.insert(0, str(PLATFORM_DIR))

from ingestion.event_emitter import create_safe_journey_event, create_verification_event
from ingestion.ingest import ingest_single_event
from ingestion.synthetic_generator import SyntheticDataGenerator


class IngestionTests(unittest.TestCase):
    def test_verification_event_structure_and_privacy(self):
        sample_analysis = {
            "risk_score": 75,
            "risk_level": "high",
            "warnings": ["The advert requests or mentions a recruitment fee."],
            "extracted": {
                "emails": ["recruiter@gmail.com"],
                "phone_numbers": ["0821234567"],
                "urls": ["https://suspicious.xyz/apply"],
                "domains": ["suspicious.xyz"],
            }
        }

        event = create_verification_event(sample_analysis, input_type="text_and_url")

        # Verify required analytical fields
        self.assertIn("event_id", event)
        self.assertIn("timestamp", event)
        self.assertEqual(event["risk_score"], 75)
        self.assertEqual(event["risk_level"], "high")
        self.assertEqual(event["warning_count"], 1)
        self.assertEqual(event["email_count"], 1)
        self.assertEqual(event["phone_count"], 1)
        self.assertEqual(event["url_count"], 1)
        self.assertEqual(event["domain_count"], 1)

        # PRIVACY BY DESIGN VERIFICATION:
        # Full text or raw messages must not be present in the event dict
        self.assertNotIn("text", event)
        self.assertNotIn("job_description", event)
        self.assertNotIn("raw_payload", event)
        # Raw warnings list must not be in telemetry — only codes
        self.assertNotIn("warnings", event)
        # warning_codes must be present instead
        self.assertIn("warning_codes", event)
        self.assertIn("warning_categories", event)
        # Email address must not appear in event
        self.assertNotIn("recruiter@gmail.com", str(event))

    def test_synthetic_generator_determinism(self):
        gen1 = SyntheticDataGenerator(seed=123)
        gen2 = SyntheticDataGenerator(seed=123)

        v1, _ = gen1.generate_dataset(num_verification_records=20, num_journey_records=5, days_back=5)
        v2, _ = gen2.generate_dataset(num_verification_records=20, num_journey_records=5, days_back=5)

        self.assertEqual(len(v1), len(v2))
        self.assertEqual(v1[0]["event_id"], v2[0]["event_id"])
        self.assertEqual(v1[0]["risk_score"], v2[0]["risk_score"])

    def test_single_event_landing_partition(self):
        event = {
            "event_id": "test_evt_001",
            "event_type": "job_verification",
            "timestamp": "2026-09-20T10:00:00+00:00",
            "risk_score": 10,
            "risk_level": "low",
        }
        file_path = ingest_single_event(event, domain="verification")
        self.assertTrue(file_path.exists())
        self.assertTrue("2026-09-20" in str(file_path))

        with open(file_path, "r", encoding="utf-8") as f:
            loaded = json.load(f)
        self.assertEqual(loaded["event_id"], "test_evt_001")


if __name__ == "__main__":
    unittest.main()
