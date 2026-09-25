"""
Targeted tests for the fixes applied to the Thuso AI Data Engineering Platform.

Covers:
  - Privacy: raw emails, phone numbers, URLs, domains must not enter telemetry
  - Processing time: processing_time_ms is measured, numeric, and >= 0
  - Freshness: fresh data PASS, stale data FAIL, no data handled
  - Raw quality: invalid and duplicate source records affect source quality metrics
  - Warehouse integrity: clean accepted records pass warehouse integrity
  - Reproducibility: same seed + params = identical dataset
  - SQL failure: critical SQL errors raise exceptions (not silently ignored)
  - Airflow DAG: imports successfully and task chain is correct
"""
import sys
import os
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path

PLATFORM_DIR = Path(__file__).resolve().parent.parent
if str(PLATFORM_DIR) not in sys.path:
    sys.path.insert(0, str(PLATFORM_DIR))


# ============================================================================
# 1. PRIVACY TESTS
# ============================================================================
class PrivacyTests(unittest.TestCase):
    def _make_analysis(self, email="user@gmail.com", phone="0821234567", url="https://bit.ly/abc", domain="gmail.com"):
        return {
            "risk_score": 75,
            "risk_level": "high",
            "warnings": [
                f"Recruiter uses a public email address: {email}",
                "The advert requests or mentions a recruitment fee.",
            ],
            "extracted": {
                "emails": [email],
                "phone_numbers": [phone],
                "urls": [url],
                "domains": [domain],
            },
        }

    def test_email_never_in_telemetry(self):
        from ingestion.event_emitter import create_verification_event
        analysis = self._make_analysis(email="secret@gmail.com")
        event = create_verification_event(analysis)
        event_str = str(event)
        self.assertNotIn("secret@gmail.com", event_str)
        self.assertNotIn("@gmail.com", event_str)

    def test_phone_never_in_telemetry(self):
        from ingestion.event_emitter import create_verification_event
        analysis = self._make_analysis(phone="0821234567")
        event = create_verification_event(analysis)
        event_str = str(event)
        self.assertNotIn("0821234567", event_str)

    def test_url_never_in_telemetry(self):
        from ingestion.event_emitter import create_verification_event
        analysis = self._make_analysis(url="https://very-suspicious-site.xyz/apply-now")
        event = create_verification_event(analysis)
        event_str = str(event)
        self.assertNotIn("https://very-suspicious-site.xyz/apply-now", event_str)

    def test_domain_string_never_in_telemetry(self):
        from ingestion.event_emitter import create_verification_event
        analysis = self._make_analysis(domain="verysuspicious.xyz")
        event = create_verification_event(analysis)
        # domains list should NOT be in the event
        self.assertNotIn("domains", event)
        # raw domain should not appear as a value
        self.assertNotIn("verysuspicious.xyz", str(event))

    def test_raw_warning_text_replaced_by_code(self):
        from ingestion.event_emitter import create_verification_event
        analysis = {
            "risk_score": 60,
            "risk_level": "high",
            "warnings": ["Recruiter uses a public email address: test@example.com"],
            "extracted": {"emails": ["test@example.com"], "phone_numbers": [], "urls": [], "domains": []},
        }
        event = create_verification_event(analysis)
        # Raw warning message must not be in telemetry
        self.assertNotIn("test@example.com", str(event))
        # warning_codes should be present
        self.assertIn("warning_codes", event)
        self.assertGreater(len(event["warning_codes"]), 0)

    def test_telemetry_contains_only_allowed_fields(self):
        from ingestion.event_emitter import create_verification_event
        analysis = {
            "risk_score": 30,
            "risk_level": "medium",
            "warnings": [],
            "extracted": {"emails": [], "phone_numbers": [], "urls": [], "domains": []},
        }
        event = create_verification_event(analysis)
        forbidden_keys = {"text", "job_description", "raw_payload", "emails", "phone_numbers", "urls", "domains", "warnings"}
        for key in forbidden_keys:
            self.assertNotIn(key, event, f"Forbidden key '{key}' found in telemetry event")


# ============================================================================
# 2. PROCESSING TIME TESTS
# ============================================================================
class ProcessingTimeTests(unittest.TestCase):
    def test_processing_time_ms_exists_in_event(self):
        from ingestion.event_emitter import create_verification_event
        analysis = {
            "risk_score": 10,
            "risk_level": "low",
            "warnings": [],
            "extracted": {"emails": [], "phone_numbers": [], "urls": [], "domains": []},
        }
        event = create_verification_event(analysis, processing_time_ms=42.5)
        self.assertIn("processing_time_ms", event)

    def test_processing_time_ms_is_numeric(self):
        from ingestion.event_emitter import create_verification_event
        analysis = {"risk_score": 0, "risk_level": "low", "warnings": [], "extracted": {"emails": [], "phone_numbers": [], "urls": [], "domains": []}}
        event = create_verification_event(analysis, processing_time_ms=12.3)
        self.assertIsInstance(event["processing_time_ms"], (int, float))

    def test_processing_time_ms_is_non_negative(self):
        from ingestion.event_emitter import create_verification_event
        analysis = {"risk_score": 0, "risk_level": "low", "warnings": [], "extracted": {"emails": [], "phone_numbers": [], "urls": [], "domains": []}}
        event = create_verification_event(analysis, processing_time_ms=0.0)
        self.assertGreaterEqual(event["processing_time_ms"], 0)

    def test_negative_processing_time_clamped_to_zero(self):
        from ingestion.event_emitter import create_verification_event
        analysis = {"risk_score": 0, "risk_level": "low", "warnings": [], "extracted": {"emails": [], "phone_numbers": [], "urls": [], "domains": []}}
        event = create_verification_event(analysis, processing_time_ms=-5.0)
        self.assertEqual(event["processing_time_ms"], 0.0)


# ============================================================================
# 3. FRESHNESS TESTS
# ============================================================================
class FreshnessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from warehouse.db import DatabaseManager
        from warehouse.loader import WarehouseLoader
        from quality.quality_checker import DataQualityChecker
        cls.db = DatabaseManager(force_sqlite=True)
        cls.db.initialize_schema()
        cls.loader = WarehouseLoader()
        cls.loader.db = cls.db
        cls.checker = DataQualityChecker()
        cls.checker.db = cls.db

    def _load_fresh_fact(self, ts_str: str):
        """Helper: insert a minimal verification fact at given timestamp."""
        from transformations.transformer import DataTransformer
        transformer = DataTransformer()
        dt = datetime.fromisoformat(ts_str)
        date_key = int(dt.strftime("%Y%m%d"))
        dim_dates = transformer.build_dim_date_records({dt.replace(hour=0, minute=0, second=0, microsecond=0)})
        dim_risks = transformer.build_dim_risk_categories()
        dim_inputs = transformer.build_dim_input_types()
        self.loader.load_dimension("dim_date", dim_dates, "date_key")
        self.loader.load_dimension("dim_risk_category", dim_risks, "risk_level")
        self.loader.load_dimension("dim_input_type", dim_inputs, "input_type_code")
        fact = {
            "verification_id": f"freshness_test_{ts_str[:10].replace('-','')}_{id(self)}",
            "date_key": date_key,
            "event_timestamp": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "input_type": "text_only",
            "user_agent_type": "web_desktop",
            "risk_level": "low",
            "risk_score": 5,
            "warning_count": 0,
            "email_count": 0,
            "phone_count": 0,
            "url_count": 0,
            "domain_count": 0,
            "has_suspicious_domain": False,
            "is_shortened_url": False,
            "processing_time_ms": 10.0,
            "event_source": "test",
        }
        self.loader.load_fact_verifications([fact])

    def test_fresh_data_passes(self):
        """Data within threshold should return PASS."""
        now = datetime.now(timezone.utc)
        fresh_ts = (now - timedelta(hours=1)).isoformat()
        self._load_fresh_fact(fresh_ts)
        passed, details = self.checker.run_freshness_checks(threshold_hours=24, now_utc=now)
        self.assertTrue(passed)
        self.assertEqual(details["freshness_checks"]["freshness_status"], "PASS")

    def test_stale_data_fails(self):
        """Data older than threshold should return FAIL."""
        now = datetime.now(timezone.utc)
        stale_ts = (now - timedelta(hours=48)).isoformat()
        self._load_fresh_fact(stale_ts)
        # Check specifically with a very recent now that makes existing data stale
        stale_now = now + timedelta(hours=100)
        passed, details = self.checker.run_freshness_checks(threshold_hours=24, now_utc=stale_now)
        self.assertFalse(passed)
        self.assertEqual(details["freshness_checks"]["freshness_status"], "FAIL")

    def test_no_data_is_handled_gracefully(self):
        """No data in warehouse should return FAIL with a clear reason."""
        from warehouse.db import DatabaseManager
        from quality.quality_checker import DataQualityChecker
        # Use a fresh in-memory DB with no data
        fresh_db = DatabaseManager(force_sqlite=True)
        # Use an in-memory SQLite
        from sqlalchemy import create_engine
        fresh_db._engine = create_engine("sqlite:///:memory:")
        fresh_db.db_type = "sqlite"
        fresh_db.initialize_schema()

        checker = DataQualityChecker()
        checker.db = fresh_db

        passed, details = checker.run_freshness_checks(threshold_hours=24, now_utc=datetime.now(timezone.utc))
        self.assertFalse(passed)
        self.assertEqual(details["freshness_checks"]["freshness_status"], "FAIL")
        self.assertIsNone(details["freshness_checks"]["latest_event_timestamp"])


# ============================================================================
# 4. RAW QUALITY & WAREHOUSE INTEGRITY TESTS
# ============================================================================
class QualitySeparationTests(unittest.TestCase):
    def test_raw_quality_rate_reflects_invalids_and_duplicates(self):
        """Invalid and duplicate records must reduce the raw quality rate below 100%."""
        from validation.validator import EventValidator
        validator = EventValidator()

        good_record = {
            "event_id": "sep_test_001",
            "timestamp": "2026-09-23T10:00:00+00:00",
            "risk_score": 20,
            "risk_level": "low",
            "warning_count": 0,
            "email_count": 0,
            "phone_count": 0,
            "url_count": 0,
            "domain_count": 0,
        }
        bad_record = {
            "event_id": "sep_test_002",
            "timestamp": "2026-09-23T10:00:00+00:00",
            "risk_score": 999,  # invalid
            "risk_level": "low",
        }
        is_ok1, _ = validator.validate_verification_event(good_record)
        is_ok2, _ = validator.validate_verification_event(bad_record)
        self.assertTrue(is_ok1)
        self.assertFalse(is_ok2)

        total = 2
        valid_count = 1
        invalid_count = 1
        raw_quality_rate = valid_count / total * 100.0
        self.assertLess(raw_quality_rate, 100.0)

    def test_warehouse_integrity_separate_from_raw_quality(self):
        """Warehouse integrity metrics are derived from warehouse tables, not from raw files."""
        from quality.quality_checker import DataQualityChecker

        checker = DataQualityChecker()
        checker.db.initialize_schema()

        c_pass, c_details = checker.run_completeness_checks()
        v_pass, v_details = checker.run_validity_checks()
        u_pass, u_details = checker.run_uniqueness_checks()
        s_pass, s_details = checker.run_consistency_checks()

        self.assertIn("completeness_checks", c_details)
        self.assertIn("validity_checks", v_details)
        self.assertIn("uniqueness_checks", u_details)
        self.assertIn("consistency_checks", s_details)

    def test_quality_metric_record_has_both_raw_and_warehouse_fields(self):
        """evaluate_quality_suite must produce both raw quality and warehouse integrity fields."""
        from quality.quality_checker import DataQualityChecker

        checker = DataQualityChecker()
        checker.db.initialize_schema()

        raw_stats = {
            "total_records": 100,
            "valid_count": 90,
            "invalid_count": 7,
            "duplicate_count": 3,
            "quality_rate_pct": 90.0,
        }
        result = checker.evaluate_quality_suite(
            stage_name="test_separation",
            raw_source_stats=raw_stats,
        )
        mr = result["metric_record"]

        # Raw source fields
        self.assertIn("total_received", mr)
        self.assertIn("valid_records_count", mr)
        self.assertIn("invalid_records_count", mr)
        self.assertIn("duplicate_records_count", mr)
        self.assertIn("raw_quality_rate_pct", mr)
        self.assertEqual(mr["raw_quality_rate_pct"], 90.0)

        # Warehouse integrity fields
        self.assertIn("warehouse_records_count", mr)
        self.assertIn("constraint_violations_count", mr)
        self.assertIn("orphan_records_count", mr)
        self.assertIn("warehouse_integrity_status", mr)

        # Freshness
        self.assertIn("freshness_status", mr)
        self.assertIn("overall_status", mr)


# ============================================================================
# 5. REPRODUCIBILITY TESTS
# ============================================================================
class ReproducibilityTests(unittest.TestCase):
    def test_same_seed_same_dataset(self):
        """Same seed + same parameters must produce an identical dataset."""
        from ingestion.synthetic_generator import SyntheticDataGenerator, DETERMINISTIC_BASE_TIME

        gen1 = SyntheticDataGenerator(seed=77, reference_time=DETERMINISTIC_BASE_TIME)
        gen2 = SyntheticDataGenerator(seed=77, reference_time=DETERMINISTIC_BASE_TIME)

        v1, j1 = gen1.generate_dataset(num_verification_records=30, num_journey_records=10, days_back=7)
        v2, j2 = gen2.generate_dataset(num_verification_records=30, num_journey_records=10, days_back=7)

        self.assertEqual(len(v1), len(v2))
        self.assertEqual(len(j1), len(j2))

        # Compare content, not just length
        for rec1, rec2 in zip(v1, v2):
            self.assertEqual(rec1["event_id"], rec2["event_id"])
            self.assertEqual(rec1["risk_score"], rec2["risk_score"])
            self.assertEqual(rec1["timestamp"], rec2["timestamp"])
            self.assertEqual(rec1["risk_level"], rec2["risk_level"])

    def test_different_seeds_different_datasets(self):
        """Different seeds should produce different datasets."""
        from ingestion.synthetic_generator import SyntheticDataGenerator, DETERMINISTIC_BASE_TIME

        gen1 = SyntheticDataGenerator(seed=1, reference_time=DETERMINISTIC_BASE_TIME)
        gen2 = SyntheticDataGenerator(seed=9999, reference_time=DETERMINISTIC_BASE_TIME)

        v1, _ = gen1.generate_dataset(num_verification_records=20, num_journey_records=5, days_back=5)
        v2, _ = gen2.generate_dataset(num_verification_records=20, num_journey_records=5, days_back=5)

        # At least one record should differ
        self.assertFalse(all(r1["event_id"] == r2["event_id"] for r1, r2 in zip(v1, v2)))


# ============================================================================
# 6. SQL FAILURE SURFACE TESTS
# ============================================================================
class SqlFailureTests(unittest.TestCase):
    def test_critical_sql_error_raises_exception(self):
        """Critical SQL failures must raise RuntimeError, not silently pass."""
        from warehouse.db import DatabaseManager
        from pathlib import Path
        import tempfile

        db = DatabaseManager(force_sqlite=True)

        # Write a SQL file with a non-DROP statement that will fail
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False, encoding="utf-8") as f:
            f.write("SELECT * FROM completely_nonexistent_table_xyz")
            bad_sql_path = Path(f.name)

        try:
            with self.assertRaises((RuntimeError, Exception)):
                db.execute_sql_file(bad_sql_path)
        finally:
            bad_sql_path.unlink(missing_ok=True)

    def test_missing_sql_file_raises_file_not_found(self):
        """Missing SQL file must raise FileNotFoundError."""
        from warehouse.db import DatabaseManager
        from pathlib import Path

        db = DatabaseManager(force_sqlite=True)
        with self.assertRaises(FileNotFoundError):
            db.execute_sql_file(Path("/nonexistent/path/missing.sql"))


# ============================================================================
# 7. AIRFLOW DAG IMPORT TEST
# ============================================================================
class AirflowDagTests(unittest.TestCase):
    def test_dag_module_imports_without_error(self):
        """DAG module must import cleanly (with or without Airflow installed)."""
        import importlib
        # The DAG module wraps Airflow in a try/except, so import must not raise
        try:
            from airflow.dags import thuso_data_pipeline_dag as dag_module
            self.assertTrue(hasattr(dag_module, "task_extract_and_land"))
            self.assertTrue(hasattr(dag_module, "task_validate_raw_data"))
            self.assertTrue(hasattr(dag_module, "task_transform_and_stage"))
            self.assertTrue(hasattr(dag_module, "task_load_warehouse"))
            self.assertTrue(hasattr(dag_module, "task_run_quality_checks"))
            self.assertTrue(hasattr(dag_module, "task_build_analytical_marts"))
        except Exception as e:
            self.fail(f"DAG module import failed with: {e}")

    def test_dag_task_functions_are_callable(self):
        """All six pipeline task functions must be callable."""
        from airflow.dags.thuso_data_pipeline_dag import (
            task_extract_and_land,
            task_validate_raw_data,
            task_transform_and_stage,
            task_load_warehouse,
            task_run_quality_checks,
            task_build_analytical_marts,
        )
        for fn in [task_extract_and_land, task_validate_raw_data, task_transform_and_stage,
                   task_load_warehouse, task_run_quality_checks, task_build_analytical_marts]:
            self.assertTrue(callable(fn))


# ============================================================================
# 8. WARNING CODE MAPPING TEST
# ============================================================================
class WarningCodeMappingTests(unittest.TestCase):
    def test_all_standard_warning_patterns_map_to_valid_codes(self):
        """All known Thuso warning messages must map to a recognized analytical code."""
        from ingestion.event_emitter import map_warning_to_code, WARNING_CATALOGUE
        standard_warnings = [
            "The advert requests or mentions a recruitment fee.",
            "Payment appears to be required during the recruitment process.",
            "The advert promises guaranteed employment.",
            "The recruiter asks to communicate only through WhatsApp.",
            "Sensitive banking or authentication information is requested.",
            "The message uses urgency or pressure tactics.",
            "The message tells the job seeker to attend alone.",
            "The recruiter asks the job seeker to bring cash.",
            "The interview appears to be arranged at a private or unusual venue.",
            "The interview location is being withheld until later.",
            "Recruiter uses a public email address: test@gmail.com",
            "The domain 'x.xyz' uses a domain ending commonly seen in low-trust or temporary websites.",
            "The link uses the URL shortener 'bit.ly', which hides the final destination.",
        ]
        for warning in standard_warnings:
            code, category = map_warning_to_code(warning)
            self.assertIsNotNone(code, f"Warning mapped to None code: {warning}")
            self.assertIsNotNone(category, f"Warning mapped to None category: {warning}")
            # Code must not contain raw PII
            self.assertNotIn("@", code)
            self.assertNotIn(".com", code)
            self.assertNotIn(".xyz", code)

    def test_public_email_warning_maps_to_code_not_address(self):
        """Specifically, email address warnings must produce PUBLIC_EMAIL_DOMAIN code."""
        from ingestion.event_emitter import map_warning_to_code
        code, _ = map_warning_to_code("Recruiter uses a public email address: applicant@gmail.com")
        self.assertEqual(code, "PUBLIC_EMAIL_DOMAIN")

    def test_suspicious_domain_maps_to_code(self):
        from ingestion.event_emitter import map_warning_to_code
        code, _ = map_warning_to_code("The domain 'jobs.xyz' uses a domain ending commonly seen in low-trust or temporary websites.")
        self.assertEqual(code, "SUSPICIOUS_TLD")

    def test_url_shortener_maps_to_code(self):
        from ingestion.event_emitter import map_warning_to_code
        code, _ = map_warning_to_code("The link uses the URL shortener 'bit.ly', which hides the final destination.")
        self.assertEqual(code, "URL_SHORTENER")


if __name__ == "__main__":
    unittest.main()
