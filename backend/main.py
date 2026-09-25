import re
from urllib.parse import urlparse

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


app = FastAPI(
    title="Thuso API",
    description="Job opportunity verification and interview safety API",
    version="0.2.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


class JobCheckRequest(BaseModel):
    text: str
    url: str | None = None


FREE_EMAIL_DOMAINS = {
    "gmail.com",
    "yahoo.com",
    "hotmail.com",
    "outlook.com",
}

SUSPICIOUS_TLDS = {
    ".xyz",
    ".top",
    ".click",
    ".work",
    ".online",
}

URL_SHORTENERS = {
    "bit.ly",
    "tinyurl.com",
    "t.co",
    "cutt.ly",
    "shorturl.at",
}

RISK_PATTERNS = [
    {
        "pattern": r"\b(registration|application|processing|admin)\s+fee\b",
        "message": "The advert requests or mentions a recruitment fee.",
        "weight": 30,
    },
    {
        "pattern": r"\bpay\b.{0,40}\b(interview|job|position|training|uniform)\b",
        "message": "Payment appears to be required during the recruitment process.",
        "weight": 30,
    },
    {
        "pattern": r"\bguaranteed\s+(job|employment|position)\b",
        "message": "The advert promises guaranteed employment.",
        "weight": 20,
    },
    {
        "pattern": r"\bwhatsapp\s+only\b",
        "message": "The recruiter asks to communicate only through WhatsApp.",
        "weight": 15,
    },
    {
        "pattern": r"\b(send|provide|share)\b.{0,30}\b(bank|banking|pin|password)\b",
        "message": "Sensitive banking or authentication information is requested.",
        "weight": 40,
    },
    {
        "pattern": r"\bact\s+now\b|\burgent\b|\bimmediately\b",
        "message": "The message uses urgency or pressure tactics.",
        "weight": 10,
    },
    {
        "pattern": r"\bcome\s+alone\b|\bdo\s+not\s+bring\s+(anyone|someone)\b",
        "message": "The message tells the job seeker to attend alone.",
        "weight": 25,
    },
    {
        "pattern": r"\bbring\b.{0,20}\bcash\b|\bcash\s+(only|required)\b",
        "message": "The recruiter asks the job seeker to bring cash.",
        "weight": 25,
    },
    {
        "pattern": (
            r"\b(meet|interview)\b.{0,45}"
            r"\b(hotel|guest\s*house|private\s+house|residence|apartment|flat)\b"
        ),
        "message": "The interview appears to be arranged at a private or unusual venue.",
        "weight": 20,
    },
    {
        "pattern": r"\b(location|address)\b.{0,30}\b(sent|shared|given)\b.{0,20}\b(later|after)\b",
        "message": "The interview location is being withheld until later.",
        "weight": 15,
    },
]


def extract_urls(text: str) -> list[str]:
    urls = re.findall(r"https?://[^\s]+", text, flags=re.IGNORECASE)
    return [url.rstrip(".,);]}'\">\"") for url in urls]


def extract_emails(text: str) -> list[str]:
    return re.findall(
        r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
        text,
    )


def extract_phone_numbers(text: str) -> list[str]:
    matches = re.findall(
        r"(?:\+27|0)[6-8][0-9](?:[\s-]?[0-9]){7}",
        text,
    )

    return list(dict.fromkeys(matches))


def normalise_url(url: str) -> str:
    cleaned = url.strip()

    if not cleaned:
        return cleaned

    if not re.match(r"^[a-z][a-z0-9+.-]*://", cleaned, flags=re.IGNORECASE):
        cleaned = f"https://{cleaned}"

    return cleaned


def get_domain(url: str) -> str | None:
    try:
        parsed = urlparse(normalise_url(url))
        domain = parsed.netloc.lower().split("@")[ -1].split(":")[0]

        if domain.startswith("www."):
            domain = domain[4:]

        return domain or None

    except ValueError:
        return None


def build_recommendations(risk_level: str, warnings: list[str]) -> list[str]:
    recommendations = [
        "Confirm the company and vacancy through an official website or independently found contact number.",
        "Do not share passwords, PINs, one-time passwords or online-banking credentials.",
    ]

    if risk_level == "high":
        recommendations.insert(
            0,
            "Pause before travelling or paying anything and independently verify the opportunity first.",
        )
    elif risk_level == "medium":
        recommendations.insert(
            0,
            "Verify the warning signals before attending the interview or sending sensitive documents.",
        )
    else:
        recommendations.insert(
            0,
            "No major warning signals were found, but continue with normal job-search safety checks.",
        )

    if any("alone" in warning.lower() or "venue" in warning.lower() for warning in warnings):
        recommendations.append(
            "Share the interview address and expected check-in time with someone you trust before travelling."
        )

    return recommendations


def analyse_job(text: str, supplied_url: str | None = None):
    lowered = text.lower()

    warnings = []
    risk_score = 0

    for rule in RISK_PATTERNS:
        if re.search(rule["pattern"], lowered, flags=re.IGNORECASE):
            warnings.append(rule["message"])
            risk_score += rule["weight"]

    emails = extract_emails(text)
    phone_numbers = extract_phone_numbers(text)
    urls = extract_urls(text)

    if supplied_url and supplied_url.strip():
        urls.append(supplied_url.strip())

    urls = list(dict.fromkeys(urls))

    for email in emails:
        email_domain = email.split("@")[-1].lower()

        if email_domain in FREE_EMAIL_DOMAINS:
            warnings.append(f"Recruiter uses a public email address: {email}")
            risk_score += 15

    domains = []

    for url in urls:
        domain = get_domain(url)

        if domain:
            domains.append(domain)

            if any(domain.endswith(tld) for tld in SUSPICIOUS_TLDS):
                warnings.append(
                    f"The domain '{domain}' uses a domain ending commonly seen "
                    "in low-trust or temporary websites."
                )
                risk_score += 15

            if domain in URL_SHORTENERS:
                warnings.append(
                    f"The link uses the URL shortener '{domain}', which hides the final destination."
                )
                risk_score += 10

    warnings = list(dict.fromkeys(warnings))
    risk_score = min(risk_score, 100)

    if risk_score >= 60:
        risk_level = "high"
    elif risk_score >= 30:
        risk_level = "medium"
    else:
        risk_level = "low"

    return {
        "risk_score": risk_score,
        "risk_level": risk_level,
        "warnings": warnings,
        "recommendations": build_recommendations(risk_level, warnings),
        "extracted": {
            "emails": emails,
            "phone_numbers": phone_numbers,
            "urls": urls,
            "domains": list(dict.fromkeys(domains)),
        },
        "disclaimer": (
            "Thuso's risk score is a screening indicator, not a guarantee that "
            "a job opportunity is legitimate or fraudulent."
        ),
    }


@app.get("/")
def root():
    return {
        "name": "Thuso API",
        "status": "running",
    }


@app.get("/health")
def health():
    return {
        "status": "healthy",
    }


@app.post("/api/verify")
def verify_job(request: JobCheckRequest):
    import time
    t0 = time.perf_counter()
    result = analyse_job(
        text=request.text,
        supplied_url=request.url,
    )
    elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)

    # Lightweight Data Platform Telemetry Hook (Privacy-Safe & Non-Blocking)
    try:
        import sys
        from pathlib import Path
        platform_dir = Path(__file__).resolve().parent.parent / "data-platform"
        if platform_dir.exists():
            if str(platform_dir) not in sys.path:
                sys.path.insert(0, str(platform_dir))
            from ingestion.event_emitter import create_verification_event
            from ingestion.ingest import ingest_single_event

            input_type = "text_and_url" if (request.text and request.url) else ("url_only" if request.url else "text_only")
            event = create_verification_event(
                analysis_result=result,
                input_type=input_type,
                processing_time_ms=elapsed_ms,
                event_source="app_live_api",
            )
            ingest_single_event(event, domain="verification")
    except Exception:
        # Guarantee that telemetry logging never interferes with or breaks user verification
        pass

    return result


