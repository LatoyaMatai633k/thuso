import random
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

PLATFORM_DIR = Path(__file__).resolve().parent.parent
if str(PLATFORM_DIR) not in sys.path:
    sys.path.insert(0, str(PLATFORM_DIR))

from config.settings import (
    DEFAULT_RANDOM_SEED,
    DEFAULT_SYNTHETIC_DAYS,
    DEFAULT_SYNTHETIC_RECORDS,
    DUPLICATE_RECORD_RATIO,
    INVALID_RECORD_RATIO,
)
from ingestion.event_emitter import WARNING_CATALOGUE

# Available warning codes from centralized catalogue
CATALOGUE_WARNING_CODES = list(WARNING_CATALOGUE.keys())

INPUT_TYPES = ["text_only", "url_only", "text_and_url", "pasted_email"]
INPUT_TYPE_WEIGHTS = [0.45, 0.20, 0.25, 0.10]

USER_AGENTS = ["web_desktop", "web_mobile", "web_tablet"]
USER_AGENT_WEIGHTS = [0.55, 0.40, 0.05]

LOCATION_ZONES = [
    "Johannesburg Central",
    "Cape Town CBD",
    "Durban Central",
    "Pretoria North",
    "Soweto West",
    "Sandton Central",
    "Gqeberha Metro",
    "Bloemfontein Central",
]

JOURNEY_STATUSES = ["completed", "completed", "completed", "cancelled", "alert_triggered"]

# Fixed default reference timestamp for 100% deterministic, reproducible seeded data generation
DETERMINISTIC_BASE_TIME = datetime(2026, 9, 23, 12, 0, 0, tzinfo=timezone.utc)


class SyntheticDataGenerator:
    def __init__(self, seed: int = DEFAULT_RANDOM_SEED, reference_time: Optional[datetime] = None):
        self.seed = seed
        self.rng = random.Random(seed)
        self.reference_time = reference_time or DETERMINISTIC_BASE_TIME

    def generate_verification_record(self, event_time: datetime) -> Dict[str, Any]:
        """Generates a realistic, privacy-safe job verification event using standardized warning codes."""
        # Risk distribution: 50% low, 30% medium, 20% high
        risk_tier = self.rng.choices(["low", "medium", "high"], weights=[0.50, 0.30, 0.20])[0]

        if risk_tier == "low":
            risk_score = self.rng.randint(0, 25)
            risk_level = "low"
            warning_count = self.rng.choices([0, 1], weights=[0.8, 0.2])[0]
        elif risk_tier == "medium":
            risk_score = self.rng.randint(30, 55)
            risk_level = "medium"
            warning_count = self.rng.randint(1, 3)
        else:
            risk_score = self.rng.randint(60, 100)
            risk_level = "high"
            warning_count = self.rng.randint(3, 6)

        selected_codes = self.rng.sample(CATALOGUE_WARNING_CODES, min(warning_count, len(CATALOGUE_WARNING_CODES)))
        selected_categories = list(dict.fromkeys(WARNING_CATALOGUE[code]["category"] for code in selected_codes))

        input_type = self.rng.choices(INPUT_TYPES, weights=INPUT_TYPE_WEIGHTS)[0]
        user_agent = self.rng.choices(USER_AGENTS, weights=USER_AGENT_WEIGHTS)[0]

        has_email = 1 if (risk_level != "low" or self.rng.random() < 0.4) else 0
        has_phone = 1 if self.rng.random() < 0.5 else 0
        has_url = 1 if input_type in ["url_only", "text_and_url"] else (1 if self.rng.random() < 0.2 else 0)
        has_domain = has_url

        has_suspicious_domain = "SUSPICIOUS_TLD" in selected_codes or (risk_tier == "high" and self.rng.random() < 0.6)
        is_shortened_url = "URL_SHORTENER" in selected_codes or (risk_tier != "low" and self.rng.random() < 0.3)

        processing_time = round(self.rng.uniform(25.0, 180.0), 2)
        event_id = str(uuid.UUID(int=self.rng.getrandbits(128)))
        iso_timestamp = event_time.isoformat()

        return {
            "event_id": event_id,
            "event_type": "job_verification",
            "event_source": "synthetic_generator",
            "timestamp": iso_timestamp,
            "event_timestamp": iso_timestamp,
            "input_type": input_type,
            "user_agent_type": user_agent,
            "processing_time_ms": processing_time,
            "risk_score": risk_score,
            "risk_level": risk_level,
            "warning_count": len(selected_codes),
            "warning_codes": selected_codes,
            "warning_categories": selected_categories,
            "email_count": has_email,
            "phone_count": has_phone,
            "url_count": has_url,
            "domain_count": has_domain,
            "has_suspicious_domain": has_suspicious_domain,
            "is_shortened_url": is_shortened_url,
        }

    def generate_safe_journey_record(self, event_time: datetime) -> Dict[str, Any]:
        """Generates a realistic, privacy-safe Safe Journey event."""
        status = self.rng.choice(JOURNEY_STATUSES)
        duration = self.rng.randint(15, 120)
        location_zone = self.rng.choice(LOCATION_ZONES)
        emergency_trigger = (status == "alert_triggered")
        check_ins = max(1, duration // 20)
        event_id = str(uuid.UUID(int=self.rng.getrandbits(128)))
        iso_timestamp = event_time.isoformat()

        return {
            "event_id": event_id,
            "event_type": "safe_journey",
            "event_source": "synthetic_generator",
            "timestamp": iso_timestamp,
            "event_timestamp": iso_timestamp,
            "journey_status": status,
            "duration_minutes": duration,
            "location_zone": location_zone,
            "emergency_trigger": emergency_trigger,
            "trusted_contacts_notified": self.rng.randint(1, 3),
            "check_in_count": check_ins,
        }

    def generate_dataset(
        self,
        num_verification_records: int = DEFAULT_SYNTHETIC_RECORDS,
        num_journey_records: int = 150,
        days_back: int = DEFAULT_SYNTHETIC_DAYS,
        reference_time: Optional[datetime] = None,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Generates full datasets for both Job Verification and Safe Journey deterministically,
        including controlled invalid and duplicate records for data quality validation.
        """
        base_time = reference_time or self.reference_time
        start_time = base_time - timedelta(days=days_back)
        total_seconds = int((base_time - start_time).total_seconds())

        verification_records: List[Dict[str, Any]] = []
        journey_records: List[Dict[str, Any]] = []

        # Generate base clean verification records with deterministic temporal distribution
        for _ in range(num_verification_records):
            random_seconds = self.rng.randint(0, total_seconds)
            event_time = start_time + timedelta(seconds=random_seconds)
            record = self.generate_verification_record(event_time)
            verification_records.append(record)

        # Generate journey records
        for _ in range(num_journey_records):
            random_seconds = self.rng.randint(0, total_seconds)
            event_time = start_time + timedelta(seconds=random_seconds)
            journey_records.append(self.generate_safe_journey_record(event_time))

        # Inject controlled duplicate records deterministically
        num_duplicates = max(1, int(num_verification_records * DUPLICATE_RECORD_RATIO))
        duplicates = self.rng.sample(verification_records, num_duplicates)
        for dup in duplicates:
            verification_records.append(dict(dup))

        # Inject controlled invalid records for quality validation demonstration
        num_invalids = max(1, int(num_verification_records * INVALID_RECORD_RATIO))
        for i in range(num_invalids):
            random_seconds = self.rng.randint(0, total_seconds)
            event_time = start_time + timedelta(seconds=random_seconds)
            bad_record = self.generate_verification_record(event_time)
            
            # Diverse validation failure modes
            mode = i % 4
            if mode == 0:
                bad_record["risk_score"] = 999  # Out of range > 100
            elif mode == 1:
                bad_record["warning_count"] = -5  # Negative count
            elif mode == 2:
                bad_record["timestamp"] = "invalid-date-format"  # Malformed timestamp
                bad_record["event_timestamp"] = "invalid-date-format"
            elif mode == 3:
                bad_record.pop("event_id", None)  # Missing primary identifier

            verification_records.append(bad_record)

        # Sort chronologically
        verification_records.sort(key=lambda r: str(r.get("timestamp", "")))
        journey_records.sort(key=lambda r: str(r.get("timestamp", "")))

        return verification_records, journey_records
