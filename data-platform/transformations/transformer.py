import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

PLATFORM_DIR = Path(__file__).resolve().parent.parent
if str(PLATFORM_DIR) not in sys.path:
    sys.path.insert(0, str(PLATFORM_DIR))

from ingestion.event_emitter import WARNING_CATALOGUE, map_warning_to_code

logger = logging.getLogger("thuso.transformation")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


def generate_date_key(dt: datetime) -> int:
    """Creates integer Date Key in YYYYMMDD format."""
    return int(dt.strftime("%Y%m%d"))


def generate_warning_code(warning_text: str) -> str:
    """Maps a warning message or returns standard warning code."""
    code, _ = map_warning_to_code(warning_text)
    return code


class DataTransformer:
    def __init__(self):
        pass

    def build_dim_date_records(self, dates: Set[datetime]) -> List[Dict[str, Any]]:
        """Builds standardized calendar dimension records."""
        dim_dates = []
        for dt in sorted(dates):
            date_key = generate_date_key(dt)
            dim_dates.append({
                "date_key": date_key,
                "full_date": dt.strftime("%Y-%m-%d"),
                "day_of_week": dt.isoweekday(),
                "day_name": dt.strftime("%A"),
                "month": dt.month,
                "month_name": dt.strftime("%B"),
                "quarter": (dt.month - 1) // 3 + 1,
                "year": dt.year,
                "is_weekend": dt.isoweekday() in [6, 7],
            })
        return dim_dates

    def build_dim_risk_categories(self) -> List[Dict[str, Any]]:
        """Builds risk category dimension."""
        return [
            {
                "risk_level": "low",
                "score_min": 0,
                "score_max": 29,
                "action_recommendation": "Proceed with standard job search safety checks.",
                "severity_tier": 1,
            },
            {
                "risk_level": "medium",
                "score_min": 30,
                "score_max": 59,
                "action_recommendation": "Verify warning signals before attending interview or sending documents.",
                "severity_tier": 2,
            },
            {
                "risk_level": "high",
                "score_min": 60,
                "score_max": 100,
                "action_recommendation": "Pause before travelling or paying anything and independently verify the opportunity.",
                "severity_tier": 3,
            },
        ]

    def build_dim_input_types(self) -> List[Dict[str, Any]]:
        """Builds input channels dimension."""
        return [
            {"input_type_code": "text_only", "display_name": "Job Description Text", "description": "Pasted text message or advert description"},
            {"input_type_code": "url_only", "display_name": "Job Link / URL", "description": "Submitted URL or career portal link"},
            {"input_type_code": "text_and_url", "display_name": "Combined Text & URL", "description": "Text description with accompanying application link"},
            {"input_type_code": "pasted_email", "display_name": "Recruiter Email", "description": "Direct recruiter email correspondence"},
        ]

    def build_dim_warning_types(self) -> List[Dict[str, Any]]:
        """Builds warning catalog dimension from centralized catalogue."""
        dim_warnings = []
        for code, info in WARNING_CATALOGUE.items():
            dim_warnings.append({
                "warning_code": code,
                "warning_text": info["name"],
                "category": info["category"],
            })
        # Include fallback category
        dim_warnings.append({
            "warning_code": "GENERAL_ANOMALY",
            "warning_text": "General Suspicious Anomaly",
            "category": "General Deception",
        })
        return dim_warnings

    def transform_verifications(self, valid_records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], Set[datetime]]:
        """
        Transforms validated raw verification events into fact records,
        warning link table records, and discovered warning dimension entries.
        """
        facts: List[Dict[str, Any]] = []
        warning_links: List[Dict[str, Any]] = []
        warning_dim_map: Dict[str, Dict[str, Any]] = {
            w["warning_code"]: w for w in self.build_dim_warning_types()
        }
        observed_dates: Set[datetime] = set()

        for record in valid_records:
            ts_str = (record.get("timestamp") or record.get("event_timestamp", "")).replace("Z", "+00:00")
            try:
                dt = datetime.fromisoformat(ts_str)
            except Exception:
                dt = datetime.utcnow()

            observed_dates.add(dt.replace(hour=0, minute=0, second=0, microsecond=0))
            date_key = generate_date_key(dt)

            verification_id = record["event_id"]
            risk_score = int(record.get("risk_score", 0))

            # Canonicalize risk level based on normalized rules
            if risk_score >= 60:
                normalized_risk_level = "high"
            elif risk_score >= 30:
                normalized_risk_level = "medium"
            else:
                normalized_risk_level = "low"

            warning_codes = record.get("warning_codes", [])
            # If warning_codes is empty but raw warnings exist, map them
            if not warning_codes and "warnings" in record:
                for w in record["warnings"]:
                    code, _ = map_warning_to_code(str(w))
                    if code not in warning_codes:
                        warning_codes.append(code)

            has_suspicious_domain = bool(record.get("has_suspicious_domain", False))
            is_shortened = bool(record.get("is_shortened_url", False))

            fact = {
                "verification_id": verification_id,
                "date_key": date_key,
                "event_timestamp": dt.strftime("%Y-%m-%d %H:%M:%S"),
                "input_type": record.get("input_type", "text_only"),
                "user_agent_type": record.get("user_agent_type", "web_desktop"),
                "risk_level": normalized_risk_level,
                "risk_score": risk_score,
                "warning_count": int(record.get("warning_count", len(warning_codes))),
                "email_count": int(record.get("email_count", 0)),
                "phone_count": int(record.get("phone_count", 0)),
                "url_count": int(record.get("url_count", 0)),
                "domain_count": int(record.get("domain_count", 0)),
                "has_suspicious_domain": has_suspicious_domain,
                "is_shortened_url": is_shortened,
                "processing_time_ms": float(record.get("processing_time_ms", 0.0)),
                "event_source": record.get("event_source", "app_verification"),
            }
            facts.append(fact)

            for code in warning_codes:
                if code not in warning_dim_map:
                    warning_dim_map[code] = {
                        "warning_code": code,
                        "warning_text": code.replace("_", " ").title(),
                        "category": "General Deception",
                    }

                warning_links.append({
                    "verification_id": verification_id,
                    "warning_code": code,
                })

        dim_warnings = list(warning_dim_map.values())
        return facts, warning_links, dim_warnings, observed_dates

    def transform_safe_journeys(self, valid_records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Set[datetime]]:
        """
        Transforms validated Safe Journey events into fact records and location dimension entries.
        """
        facts: List[Dict[str, Any]] = []
        location_map: Dict[str, Dict[str, Any]] = {}
        observed_dates: Set[datetime] = set()

        for record in valid_records:
            ts_str = (record.get("timestamp") or record.get("event_timestamp", "")).replace("Z", "+00:00")
            try:
                dt = datetime.fromisoformat(ts_str)
            except Exception:
                dt = datetime.utcnow()

            observed_dates.add(dt.replace(hour=0, minute=0, second=0, microsecond=0))
            date_key = generate_date_key(dt)

            zone = record.get("location_zone", "Unspecified Area")
            if zone not in location_map:
                province = "Gauteng" if ("Johannesburg" in zone or "Pretoria" in zone or "Soweto" in zone or "Sandton" in zone) else (
                    "Western Cape" if "Cape Town" in zone else (
                        "KwaZulu-Natal" if "Durban" in zone else (
                            "Eastern Cape" if "Gqeberha" in zone else "Free State"
                        )
                    )
                )
                location_map[zone] = {
                    "location_zone": zone,
                    "province": province,
                    "metro_area": zone.split()[0],
                }

            facts.append({
                "journey_id": record["event_id"],
                "date_key": date_key,
                "event_timestamp": dt.strftime("%Y-%m-%d %H:%M:%S"),
                "location_zone": zone,
                "journey_status": record.get("journey_status", "completed"),
                "duration_minutes": int(record.get("duration_minutes", 30)),
                "emergency_trigger": bool(record.get("emergency_trigger", False)),
                "trusted_contacts_notified": int(record.get("trusted_contacts_notified", 1)),
                "check_in_count": int(record.get("check_in_count", 1)),
                "event_source": record.get("event_source", "synthetic_generator"),
            })

        dim_locations = list(location_map.values())
        return facts, dim_locations, observed_dates
