import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# Standardized warning code and category catalogue based on Thuso verification engine
WARNING_CATALOGUE = {
    "RECRUITMENT_FEE": {
        "code": "RECRUITMENT_FEE",
        "category": "Financial / Fee",
        "name": "Recruitment Fee Requested",
        "description": "The advert requests or mentions an upfront recruitment, application, or registration fee.",
    },
    "PAYMENT_REQUEST": {
        "code": "PAYMENT_REQUEST",
        "category": "Financial / Fee",
        "name": "Payment for Job/Training",
        "description": "Payment appears to be required during the recruitment process.",
    },
    "GUARANTEED_JOB": {
        "code": "GUARANTEED_JOB",
        "category": "General Deception",
        "name": "Guaranteed Employment Promise",
        "description": "The advert promises guaranteed employment without standard vetting.",
    },
    "WHATSAPP_ONLY": {
        "code": "WHATSAPP_ONLY",
        "category": "Communication / Domain",
        "name": "WhatsApp Only Communication",
        "description": "The recruiter asks to communicate exclusively through WhatsApp.",
    },
    "BANKING_INFO_REQUEST": {
        "code": "BANKING_INFO_REQUEST",
        "category": "Financial / Fee",
        "name": "Banking / Sensitive Info Requested",
        "description": "Sensitive banking, PIN, or authentication details are requested.",
    },
    "URGENCY": {
        "code": "URGENCY",
        "category": "General Deception",
        "name": "Urgency / Pressure Tactics",
        "description": "The message uses artificial urgency or pressure tactics.",
    },
    "ATTEND_ALONE": {
        "code": "ATTEND_ALONE",
        "category": "Physical Safety",
        "name": "Instructed to Attend Alone",
        "description": "The message instructs the job seeker to attend the interview alone.",
    },
    "BRING_CASH": {
        "code": "BRING_CASH",
        "category": "Financial / Fee",
        "name": "Requested to Bring Cash",
        "description": "The recruiter asks the job seeker to bring cash in person.",
    },
    "PRIVATE_VENUE": {
        "code": "PRIVATE_VENUE",
        "category": "Physical Safety",
        "name": "Private / Unusual Interview Venue",
        "description": "The interview is arranged at a private residence, guest house, or unusual venue.",
    },
    "WITHHELD_LOCATION": {
        "code": "WITHHELD_LOCATION",
        "category": "Physical Safety",
        "name": "Interview Location Withheld",
        "description": "The interview address is withheld until later.",
    },
    "PUBLIC_EMAIL_DOMAIN": {
        "code": "PUBLIC_EMAIL_DOMAIN",
        "category": "Communication / Domain",
        "name": "Public / Free Email Domain",
        "description": "The recruiter uses a public webmail domain (e.g. Gmail, Yahoo, Hotmail).",
    },
    "SUSPICIOUS_TLD": {
        "code": "SUSPICIOUS_TLD",
        "category": "Communication / Domain",
        "name": "Suspicious Top-Level Domain",
        "description": "The link uses a domain ending commonly associated with low-trust or disposable sites.",
    },
    "URL_SHORTENER": {
        "code": "URL_SHORTENER",
        "category": "Communication / Domain",
        "name": "URL Shortener Used",
        "description": "The opportunity link uses a URL shortener that masks the real destination.",
    },
}


def map_warning_to_code(warning_text: str) -> Tuple[str, str]:
    """
    Deterministically sanitizes and maps a raw user-facing warning message
    into a privacy-safe analytical warning code and category.
    """
    w_lower = warning_text.lower()

    if "fee" in w_lower or "registration" in w_lower or "admin fee" in w_lower:
        return "RECRUITMENT_FEE", WARNING_CATALOGUE["RECRUITMENT_FEE"]["category"]
    if "payment" in w_lower or "pay" in w_lower:
        return "PAYMENT_REQUEST", WARNING_CATALOGUE["PAYMENT_REQUEST"]["category"]
    if "guaranteed" in w_lower:
        return "GUARANTEED_JOB", WARNING_CATALOGUE["GUARANTEED_JOB"]["category"]
    if "whatsapp" in w_lower:
        return "WHATSAPP_ONLY", WARNING_CATALOGUE["WHATSAPP_ONLY"]["category"]
    if "bank" in w_lower or "pin" in w_lower or "password" in w_lower:
        return "BANKING_INFO_REQUEST", WARNING_CATALOGUE["BANKING_INFO_REQUEST"]["category"]
    if "urgency" in w_lower or "urgent" in w_lower or "act now" in w_lower or "immediately" in w_lower:
        return "URGENCY", WARNING_CATALOGUE["URGENCY"]["category"]
    if "alone" in w_lower:
        return "ATTEND_ALONE", WARNING_CATALOGUE["ATTEND_ALONE"]["category"]
    if "cash" in w_lower:
        return "BRING_CASH", WARNING_CATALOGUE["BRING_CASH"]["category"]
    if "venue" in w_lower or "hotel" in w_lower or "guest house" in w_lower or "residence" in w_lower or "house" in w_lower:
        return "PRIVATE_VENUE", WARNING_CATALOGUE["PRIVATE_VENUE"]["category"]
    if "withheld" in w_lower or ("location" in w_lower and ("later" in w_lower or "after" in w_lower)):
        return "WITHHELD_LOCATION", WARNING_CATALOGUE["WITHHELD_LOCATION"]["category"]
    if "public email" in w_lower or "email address" in w_lower or "@" in w_lower:
        return "PUBLIC_EMAIL_DOMAIN", WARNING_CATALOGUE["PUBLIC_EMAIL_DOMAIN"]["category"]
    if "domain ending" in w_lower or "low-trust" in w_lower or "tld" in w_lower:
        return "SUSPICIOUS_TLD", WARNING_CATALOGUE["SUSPICIOUS_TLD"]["category"]
    if "shortener" in w_lower or "shorturl" in w_lower or "bit.ly" in w_lower or "tinyurl" in w_lower:
        return "URL_SHORTENER", WARNING_CATALOGUE["URL_SHORTENER"]["category"]

    # Fallback to general deception category
    return "GENERAL_ANOMALY", "General Deception"


def create_verification_event(
    analysis_result: Dict[str, Any],
    input_type: str = "text_and_url",
    user_agent_type: str = "web_desktop",
    processing_time_ms: float = 0.0,
    timestamp: Optional[str] = None,
    event_id: Optional[str] = None,
    event_source: str = "app_verification",
) -> Dict[str, Any]:
    """
    Constructs a privacy-safe verification event from the analysis result.
    
    PRIVACY GUARANTEE:
    - Raw input text, job descriptions, emails, phone numbers, and URLs are NEVER stored.
    - Raw warning strings containing embedded PII/domains are converted into standardized analytical codes.
    - Only derived aggregate indicators, risk scores, counts, and category labels are retained.
    """
    if timestamp is None:
        timestamp = datetime.now(timezone.utc).isoformat()
    if event_id is None:
        event_id = str(uuid.uuid4())

    extracted = analysis_result.get("extracted", {})
    email_count = len(extracted.get("emails", []))
    phone_count = len(extracted.get("phone_numbers", []))
    url_count = len(extracted.get("urls", []))
    domain_count = len(extracted.get("domains", []))

    raw_domains = extracted.get("domains", [])
    has_suspicious_domain = any(
        any(d.endswith(ext) for ext in [".xyz", ".top", ".click", ".work", ".online"])
        for d in raw_domains
    )
    is_shortened_url = any(
        d in {"bit.ly", "tinyurl.com", "t.co", "cutt.ly", "shorturl.at"}
        for d in raw_domains
    )

    # Derive standardized warning codes and categories
    raw_warnings = analysis_result.get("warnings", [])
    warning_codes: List[str] = []
    warning_categories: List[str] = []

    for w in raw_warnings:
        code, cat = map_warning_to_code(str(w))
        if code not in warning_codes:
            warning_codes.append(code)
        if cat not in warning_categories:
            warning_categories.append(cat)

    # Ensure flags align if warning rules triggered
    if "SUSPICIOUS_TLD" in warning_codes:
        has_suspicious_domain = True
    if "URL_SHORTENER" in warning_codes:
        is_shortened_url = True

    return {
        "event_id": event_id,
        "event_type": "job_verification",
        "event_source": event_source,
        "timestamp": timestamp,
        "event_timestamp": timestamp,
        "input_type": input_type,
        "user_agent_type": user_agent_type,
        "processing_time_ms": max(0.0, round(float(processing_time_ms), 2)),
        "risk_score": int(analysis_result.get("risk_score", 0)),
        "risk_level": str(analysis_result.get("risk_level", "low")).lower(),
        "warning_count": len(raw_warnings),
        "warning_codes": warning_codes,
        "warning_categories": warning_categories,
        "email_count": email_count,
        "phone_count": phone_count,
        "url_count": url_count,
        "domain_count": domain_count,
        "has_suspicious_domain": has_suspicious_domain,
        "is_shortened_url": is_shortened_url,
    }


def create_safe_journey_event(
    journey_id: Optional[str] = None,
    status: str = "completed",
    duration_minutes: int = 45,
    location_zone: str = "Johannesburg Central",
    emergency_trigger: bool = False,
    trusted_contacts_notified: int = 1,
    check_in_count: int = 3,
    timestamp: Optional[str] = None,
    event_source: str = "synthetic_generator",
) -> Dict[str, Any]:
    """
    Constructs a privacy-safe Safe Journey event.
    
    PRIVACY GUARANTEE:
    Precise GPS coordinates, addresses, and personal contact info are excluded.
    Only coarse generalized region zones and operational safety metrics are recorded.
    """
    if timestamp is None:
        timestamp = datetime.now(timezone.utc).isoformat()
    if journey_id is None:
        journey_id = str(uuid.uuid4())

    return {
        "event_id": journey_id,
        "event_type": "safe_journey",
        "event_source": event_source,
        "timestamp": timestamp,
        "event_timestamp": timestamp,
        "journey_status": status,
        "duration_minutes": duration_minutes,
        "location_zone": location_zone,
        "emergency_trigger": emergency_trigger,
        "trusted_contacts_notified": trusted_contacts_notified,
        "check_in_count": check_in_count,
    }
