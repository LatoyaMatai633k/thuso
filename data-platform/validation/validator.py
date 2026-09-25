import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

PLATFORM_DIR = Path(__file__).resolve().parent.parent
if str(PLATFORM_DIR) not in sys.path:
    sys.path.insert(0, str(PLATFORM_DIR))

from config.settings import LOGS_DIR, RAW_DATA_DIR

logger = logging.getLogger("thuso.validation")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


class EventValidator:
    def __init__(self):
        self.seen_event_ids: Set[str] = set()

    def validate_verification_event(self, record: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Validates an individual verification record against data contracts.
        Returns (is_valid, error_reason).
        """
        # 1. Primary Identifier
        event_id = record.get("event_id")
        if not event_id or not isinstance(event_id, str) or not event_id.strip():
            return False, "Missing or invalid 'event_id'"

        # 2. Timestamp Format
        ts_str = record.get("timestamp") or record.get("event_timestamp")
        if not ts_str or not isinstance(ts_str, str):
            return False, "Missing or invalid 'timestamp' string"
        try:
            datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except Exception:
            return False, f"Timestamp '{ts_str}' is not in valid ISO-8601 format"

        # 3. Numeric Risk Score Range [0, 100]
        risk_score = record.get("risk_score")
        if risk_score is None or not isinstance(risk_score, (int, float)):
            return False, "Risk score must be a numeric integer or float"
        if risk_score < 0 or risk_score > 100:
            return False, f"Risk score {risk_score} is outside acceptable range [0, 100]"

        # 4. Risk Level Categorization
        risk_level = str(record.get("risk_level", "")).lower()
        if risk_level not in {"low", "medium", "high"}:
            return False, f"Invalid risk_level '{risk_level}', expected low, medium, or high"

        # 5. Non-negative Counts
        for count_field in ["warning_count", "email_count", "phone_count", "url_count", "domain_count"]:
            val = record.get(count_field, 0)
            if not isinstance(val, (int, float)) or val < 0:
                return False, f"Count field '{count_field}' must be a non-negative number, got {val}"

        # 6. Non-negative processing time if present
        proc_time = record.get("processing_time_ms")
        if proc_time is not None and (not isinstance(proc_time, (int, float)) or proc_time < 0):
            return False, f"Processing time must be non-negative, got {proc_time}"

        return True, "Valid"

    def validate_safe_journey_event(self, record: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Validates an individual Safe Journey event against data contracts.
        """
        event_id = record.get("event_id")
        if not event_id or not isinstance(event_id, str) or not event_id.strip():
            return False, "Missing or invalid 'event_id'"

        ts_str = record.get("timestamp") or record.get("event_timestamp")
        if not ts_str or not isinstance(ts_str, str):
            return False, "Missing or invalid 'timestamp'"
        try:
            datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except Exception:
            return False, f"Malformed timestamp '{ts_str}'"

        duration = record.get("duration_minutes", 0)
        if not isinstance(duration, (int, float)) or duration < 0:
            return False, f"Invalid duration_minutes: {duration}"

        status = record.get("journey_status")
        if status and str(status).lower() not in {"completed", "cancelled", "alert_triggered", "in_progress"}:
            return False, f"Invalid journey_status: '{status}'"

        for count_field in ["trusted_contacts_notified", "check_in_count"]:
            val = record.get(count_field, 0)
            if not isinstance(val, (int, float)) or val < 0:
                return False, f"Count field '{count_field}' must be non-negative, got {val}"

        return True, "Valid"

    def validate_raw_domain(self, domain_dir: Path, domain_type: str = "verification") -> Dict[str, Any]:
        """
        Scans all raw JSON partition files for a domain and separates them into:
        - valid_records
        - invalid_records (with failure reasons)
        - duplicate_records
        """
        valid_records: List[Dict[str, Any]] = []
        invalid_records: List[Dict[str, Any]] = []
        duplicate_records: List[Dict[str, Any]] = []
        local_seen_ids: Set[str] = set()

        json_files = list(domain_dir.glob("**/*.json"))
        logger.info("Starting validation for domain '%s' across %d files...", domain_type, len(json_files))

        for file_path in json_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    record = json.load(f)
            except Exception as e:
                invalid_records.append({
                    "raw_file": str(file_path),
                    "error": f"Failed to parse JSON file: {e}",
                    "record": None
                })
                continue

            event_id = record.get("event_id")

            # Check uniqueness / duplicates
            if event_id and (event_id in local_seen_ids or event_id in self.seen_event_ids):
                duplicate_records.append({
                    "event_id": event_id,
                    "file": str(file_path),
                    "record": record,
                    "reason": "Duplicate event_id detected in raw landing layer"
                })
                continue

            # Run contract checks
            if domain_type == "verification":
                is_valid, reason = self.validate_verification_event(record)
            else:
                is_valid, reason = self.validate_safe_journey_event(record)

            if is_valid:
                if event_id:
                    local_seen_ids.add(event_id)
                    self.seen_event_ids.add(event_id)
                valid_records.append(record)
            else:
                invalid_records.append({
                    "event_id": event_id,
                    "file": str(file_path),
                    "reason": reason,
                    "record": record
                })

        total_checked = len(valid_records) + len(invalid_records) + len(duplicate_records)
        quality_rate = (len(valid_records) / total_checked * 100.0) if total_checked > 0 else 100.0

        summary = {
            "domain": domain_type,
            "total_records": total_checked,
            "valid_count": len(valid_records),
            "invalid_count": len(invalid_records),
            "duplicate_count": len(duplicate_records),
            "quality_rate_pct": round(quality_rate, 2),
            "validation_timestamp": datetime.now(timezone.utc).isoformat(),
        }

        # Write audit validation log
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        log_file = LOGS_DIR / f"validation_{domain_type}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
        with open(log_file, "w", encoding="utf-8") as f:
            json.dump({
                "summary": summary,
                "invalid_samples": invalid_records[:20],
                "duplicate_samples": duplicate_records[:20],
            }, f, indent=2)

        logger.info(
            "Validation Summary for %s -> Total: %d | Valid: %d | Invalid: %d | Duplicates: %d | Raw Quality Rate: %.2f%%",
            domain_type, total_checked, len(valid_records), len(invalid_records), len(duplicate_records), quality_rate
        )

        return {
            "summary": summary,
            "valid_records": valid_records,
            "invalid_records": invalid_records,
            "duplicate_records": duplicate_records,
        }


def validate_all_raw_data() -> Dict[str, Any]:
    """Orchestrates validation over all raw landing directories."""
    validator = EventValidator()
    results = {}

    verification_dir = RAW_DATA_DIR / "verification"
    if verification_dir.exists():
        results["verification"] = validator.validate_raw_domain(verification_dir, "verification")

    safe_journey_dir = RAW_DATA_DIR / "safe_journey"
    if safe_journey_dir.exists():
        results["safe_journey"] = validator.validate_raw_domain(safe_journey_dir, "safe_journey")

    return results
