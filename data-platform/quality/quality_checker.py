import logging
import os
import sys
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from sqlalchemy import text

PLATFORM_DIR = Path(__file__).resolve().parent.parent
if str(PLATFORM_DIR) not in sys.path:
    sys.path.insert(0, str(PLATFORM_DIR))

from warehouse.db import db_manager
from warehouse.loader import WarehouseLoader

logger = logging.getLogger("thuso.quality")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# -----------------------------------------------------------------------
# Configurable freshness threshold (default 24 hours)
# Override via environment variable FRESHNESS_THRESHOLD_HOURS
# -----------------------------------------------------------------------
DEFAULT_FRESHNESS_THRESHOLD_HOURS = 24


def get_freshness_threshold() -> int:
    """Returns the configured freshness threshold in hours."""
    return int(os.getenv("FRESHNESS_THRESHOLD_HOURS", str(DEFAULT_FRESHNESS_THRESHOLD_HOURS)))


class DataQualityChecker:
    def __init__(self):
        self.db = db_manager
        self.loader = WarehouseLoader()

    # ------------------------------------------------------------------
    # WAREHOUSE INTEGRITY CHECKS
    # (applied to already-accepted, warehouse-resident records)
    # ------------------------------------------------------------------

    def run_completeness_checks(self) -> Tuple[bool, Dict[str, Any]]:
        """Verifies required fields in warehouse fact tables are not NULL."""
        engine = self.db.get_engine()
        checks = {}
        passed = True

        query = """
        SELECT
            SUM(CASE WHEN verification_id IS NULL THEN 1 ELSE 0 END) as null_ids,
            SUM(CASE WHEN date_key IS NULL THEN 1 ELSE 0 END) as null_dates,
            SUM(CASE WHEN risk_score IS NULL THEN 1 ELSE 0 END) as null_scores,
            SUM(CASE WHEN risk_level IS NULL THEN 1 ELSE 0 END) as null_levels
        FROM fact_verifications
        """
        with engine.connect() as conn:
            row = conn.execute(text(query)).fetchone()
            if row:
                null_ids, null_dates, null_scores, null_levels = row
                null_count = (null_ids or 0) + (null_dates or 0) + (null_scores or 0) + (null_levels or 0)
                checks["null_critical_fields"] = null_count
                if null_count > 0:
                    passed = False

        return passed, {"completeness_checks": checks}

    def run_validity_checks(self) -> Tuple[bool, Dict[str, Any]]:
        """Verifies risk score [0, 100], non-negative counts, valid risk levels in warehouse."""
        engine = self.db.get_engine()
        checks = {}
        passed = True

        query = """
        SELECT
            SUM(CASE WHEN risk_score < 0 OR risk_score > 100 THEN 1 ELSE 0 END) as invalid_scores,
            SUM(CASE WHEN warning_count < 0 OR email_count < 0 OR phone_count < 0 OR url_count < 0 THEN 1 ELSE 0 END) as negative_counts,
            SUM(CASE WHEN risk_level NOT IN ('low', 'medium', 'high') THEN 1 ELSE 0 END) as invalid_tiers
        FROM fact_verifications
        """
        with engine.connect() as conn:
            row = conn.execute(text(query)).fetchone()
            if row:
                inv_scores, neg_counts, inv_tiers = row
                inv_count = (inv_scores or 0) + (neg_counts or 0) + (inv_tiers or 0)
                checks["invalid_domain_values"] = inv_count
                if inv_count > 0:
                    passed = False

        return passed, {"validity_checks": checks}

    def run_uniqueness_checks(self) -> Tuple[bool, Dict[str, Any]]:
        """Verifies 0 duplicate primary keys in warehouse fact tables."""
        engine = self.db.get_engine()
        checks = {}
        passed = True

        query = """
        SELECT COUNT(*) - COUNT(DISTINCT verification_id) as dup_count
        FROM fact_verifications
        """
        with engine.connect() as conn:
            row = conn.execute(text(query)).fetchone()
            dup_count = row[0] if row else 0
            checks["duplicate_verifications"] = dup_count
            if dup_count > 0:
                passed = False

        return passed, {"uniqueness_checks": checks}

    def run_consistency_checks(self) -> Tuple[bool, Dict[str, Any]]:
        """Verifies referential integrity across dimension tables (orphan checks)."""
        engine = self.db.get_engine()
        checks = {}
        passed = True

        query = """
        SELECT
            SUM(CASE WHEN d.date_key IS NULL THEN 1 ELSE 0 END) as orphaned_dates,
            SUM(CASE WHEN r.risk_level IS NULL THEN 1 ELSE 0 END) as orphaned_risks,
            SUM(CASE WHEN i.input_type_code IS NULL THEN 1 ELSE 0 END) as orphaned_inputs
        FROM fact_verifications f
        LEFT JOIN dim_date d ON f.date_key = d.date_key
        LEFT JOIN dim_risk_category r ON f.risk_level = r.risk_level
        LEFT JOIN dim_input_type i ON f.input_type = i.input_type_code
        """
        with engine.connect() as conn:
            row = conn.execute(text(query)).fetchone()
            if row:
                orph_d, orph_r, orph_i = row
                orph_total = (orph_d or 0) + (orph_r or 0) + (orph_i or 0)
                checks["referential_integrity_violations"] = orph_total
                if orph_total > 0:
                    passed = False

        return passed, {"consistency_checks": checks}

    def run_freshness_checks(
        self,
        threshold_hours: Optional[int] = None,
        now_utc: Optional[datetime] = None,
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Checks data freshness using a configurable threshold.

        Steps:
        1. Find the latest event_timestamp in fact_verifications.
        2. Compare it with current UTC time.
        3. Calculate the event age in hours.
        4. Return PASS if age <= threshold, FAIL if age > threshold.
        5. If no data exists, return clearly handled result (FAIL with explanation).
        """
        threshold = threshold_hours if threshold_hours is not None else get_freshness_threshold()
        current_time = now_utc or datetime.now(timezone.utc)

        engine = self.db.get_engine()
        query = "SELECT MAX(event_timestamp) FROM fact_verifications"
        with engine.connect() as conn:
            row = conn.execute(text(query)).fetchone()
            latest_ts_str = row[0] if row and row[0] else None

        if latest_ts_str is None:
            logger.warning("Freshness check: No data found in fact_verifications.")
            return False, {
                "freshness_checks": {
                    "latest_event_timestamp": None,
                    "current_utc": current_time.isoformat(),
                    "age_hours": None,
                    "threshold_hours": threshold,
                    "freshness_status": "FAIL",
                    "freshness_reason": "No data in warehouse — cannot evaluate freshness.",
                }
            }

        try:
            latest_ts = datetime.fromisoformat(str(latest_ts_str).replace("Z", "+00:00"))
            # Make timezone-aware if naive
            if latest_ts.tzinfo is None:
                latest_ts = latest_ts.replace(tzinfo=timezone.utc)
        except Exception:
            return False, {
                "freshness_checks": {
                    "latest_event_timestamp": latest_ts_str,
                    "freshness_status": "FAIL",
                    "freshness_reason": f"Could not parse latest timestamp: {latest_ts_str}",
                }
            }

        age_td = current_time - latest_ts
        age_hours = age_td.total_seconds() / 3600.0
        is_fresh = age_hours <= threshold

        freshness_status = "PASS" if is_fresh else "FAIL"
        logger.info(
            "Freshness check: Latest event %s | Age: %.2fh | Threshold: %dh | Status: %s",
            latest_ts_str, age_hours, threshold, freshness_status
        )

        return is_fresh, {
            "freshness_checks": {
                "latest_event_timestamp": str(latest_ts_str),
                "current_utc": current_time.isoformat(),
                "age_hours": round(age_hours, 2),
                "threshold_hours": threshold,
                "freshness_status": freshness_status,
            }
        }

    # ------------------------------------------------------------------
    # FULL QUALITY SUITE
    # ------------------------------------------------------------------

    def evaluate_quality_suite(
        self,
        stage_name: str = "post_load_verification",
        raw_source_stats: Optional[Dict[str, Any]] = None,
        freshness_threshold_hours: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Executes the full data quality suite and records results.

        Produces clearly separated metrics:
          - Raw/Source quality: records received, valid, invalid, duplicates, raw_quality_rate
            (from raw_source_stats if provided, otherwise warehouse counts only)
          - Warehouse integrity: constraint violations, orphan records, integrity status
          - Freshness: age of latest event vs configurable threshold
        """
        logger.info("Executing Data Quality Suite for stage: %s", stage_name)

        # --- Warehouse integrity checks ---
        c_pass, c_details = self.run_completeness_checks()
        v_pass, v_details = self.run_validity_checks()
        u_pass, u_details = self.run_uniqueness_checks()
        s_pass, s_details = self.run_consistency_checks()
        f_pass, f_details = self.run_freshness_checks(threshold_hours=freshness_threshold_hours)

        warehouse_integrity_ok = c_pass and v_pass and u_pass and s_pass
        overall_ok = warehouse_integrity_ok and f_pass

        # Warehouse record count
        engine = self.db.get_engine()
        with engine.connect() as conn:
            res = conn.execute(text("SELECT COUNT(*) FROM fact_verifications")).fetchone()
            warehouse_records = res[0] if res else 0

        # Constraint violations and orphan counts
        constraint_violations = (
            v_details["validity_checks"].get("invalid_domain_values", 0) +
            c_details["completeness_checks"].get("null_critical_fields", 0)
        )
        orphan_records = s_details["consistency_checks"].get("referential_integrity_violations", 0)
        warehouse_integrity_status = "PASS" if warehouse_integrity_ok else "FAIL"
        freshness_status = f_details["freshness_checks"].get("freshness_status", "FAIL")

        # --- Raw/Source quality stats ---
        if raw_source_stats:
            total_received = raw_source_stats.get("total_records", warehouse_records)
            valid_source = raw_source_stats.get("valid_count", warehouse_records)
            invalid_source = raw_source_stats.get("invalid_count", 0)
            duplicate_source = raw_source_stats.get("duplicate_count", 0)
            raw_quality_rate = raw_source_stats.get("quality_rate_pct",
                (valid_source / total_received * 100.0) if total_received > 0 else 100.0)
        else:
            # Fallback: report warehouse counts as proxy
            total_received = warehouse_records
            valid_source = warehouse_records
            invalid_source = 0
            duplicate_source = 0
            raw_quality_rate = 100.0

        metric_id = f"dq_{uuid.uuid4().hex[:8]}"
        run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        metric_record = {
            "metric_id": metric_id,
            "run_timestamp": run_ts,
            "pipeline_stage": stage_name,
            # Raw source quality
            "total_received": total_received,
            "valid_records_count": valid_source,
            "invalid_records_count": invalid_source,
            "duplicate_records_count": duplicate_source,
            "raw_quality_rate_pct": round(raw_quality_rate, 2),
            # Warehouse integrity
            "warehouse_records_count": warehouse_records,
            "constraint_violations_count": constraint_violations,
            "orphan_records_count": orphan_records,
            "warehouse_integrity_status": warehouse_integrity_status,
            # Freshness
            "freshness_status": freshness_status,
            "overall_status": "PASSED" if overall_ok else "FAILED",
        }

        self.loader.record_quality_metric(metric_record)

        logger.info(
            "Quality Suite -> Stage: %s | Raw Quality: %.2f%% (%d received, %d valid, %d invalid, %d dup) "
            "| Warehouse: %d records | Violations: %d | Orphans: %d | Integrity: %s | Freshness: %s | Overall: %s",
            stage_name,
            raw_quality_rate, total_received, valid_source, invalid_source, duplicate_source,
            warehouse_records, constraint_violations, orphan_records,
            warehouse_integrity_status, freshness_status, metric_record["overall_status"]
        )

        return {
            "metric_record": metric_record,
            "details": {
                **c_details,
                **v_details,
                **u_details,
                **s_details,
                **f_details,
            }
        }
