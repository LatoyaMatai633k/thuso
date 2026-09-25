import sys
import unittest
from pathlib import Path

PLATFORM_DIR = Path(__file__).resolve().parent.parent
if str(PLATFORM_DIR) not in sys.path:
    sys.path.insert(0, str(PLATFORM_DIR))

from validation.validator import EventValidator


class ValidationTests(unittest.TestCase):
    def setUp(self):
        self.validator = EventValidator()

    def test_valid_record_passes(self):
        record = {
            "event_id": "valid_123",
            "timestamp": "2026-09-21T14:30:00+00:00",
            "risk_score": 45,
            "risk_level": "medium",
            "warning_count": 2,
            "email_count": 1,
            "phone_count": 0,
            "url_count": 1,
            "domain_count": 1,
        }
        is_valid, reason = self.validator.validate_verification_event(record)
        self.assertTrue(is_valid)
        self.assertEqual(reason, "Valid")

    def test_out_of_bounds_risk_score_rejected(self):
        record = {
            "event_id": "bad_score_1",
            "timestamp": "2026-09-21T14:30:00+00:00",
            "risk_score": 150,  # Invalid: > 100
            "risk_level": "high",
        }
        is_valid, reason = self.validator.validate_verification_event(record)
        self.assertFalse(is_valid)
        self.assertIn("outside acceptable range", reason)

    def test_negative_counts_rejected(self):
        record = {
            "event_id": "bad_count_1",
            "timestamp": "2026-09-21T14:30:00+00:00",
            "risk_score": 20,
            "risk_level": "low",
            "warning_count": -3,  # Invalid: negative count
        }
        is_valid, reason = self.validator.validate_verification_event(record)
        self.assertFalse(is_valid)
        self.assertIn("non-negative", reason)

    def test_missing_event_id_rejected(self):
        record = {
            "timestamp": "2026-09-21T14:30:00+00:00",
            "risk_score": 20,
            "risk_level": "low",
        }
        is_valid, reason = self.validator.validate_verification_event(record)
        self.assertFalse(is_valid)
        self.assertIn("Missing or invalid 'event_id'", reason)


if __name__ == "__main__":
    unittest.main()
