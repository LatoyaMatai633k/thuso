# 🛡️ Thuso AI Data Engineering Platform

The **Thuso AI Data Engineering Platform** is an analytical warehouse and ETL pipeline designed to collect, validate, transform, store, and visualize job verification telemetry and safety journey activity while enforcing **Privacy by Design**.

> **Note:** All verification events recorded by this platform are **derived analytical metrics** — no raw job descriptions, names, email addresses, phone numbers, URLs, or any personal identifiable information is stored. Safe Journey historical analytics uses privacy-safe **synthetic data** (see [Synthetic Data section](#-synthetic-data-disclosure)).

---

## 🏛️ Architecture Overview

```
                      THUSO APPLICATION
                     (FastAPI + Frontend)
                              │
                              ▼
                  Privacy-Safe Telemetry Only
               (derived codes, counts, scores)
                              │
                              ▼
                     INGESTION LAYER
              (Clean Batch Landing — partitioned)
                              │
                              ▼
                      RAW DATA LAYER
               (Partitioned Date Folders JSON)
                              │
                              ▼
                     VALIDATION LAYER
               (Contracts & Quality Quarantine)
                              │
                              ▼
                  TRANSFORMATION LAYER
                (Star Schema Modeling & Cleansing)
                              │
                              ▼
                     DATA WAREHOUSE
            (PostgreSQL / SQLite Star Schema)
                              │
                ┌─────────────┴─────────────┐
                ▼                           ▼
          DATA QUALITY                  ANALYTICS
    (Raw Quality + Warehouse        (Analytical Marts)
        Integrity + Freshness)
                │                           │
                └─────────────┬─────────────┘
                              ▼
                     STREAMLIT DASHBOARD
                 (Executive & Risk Visuals)

                      APACHE AIRFLOW
                  (Pipeline Orchestration)
```

---

## 🔒 Privacy by Design

Thuso is built for vulnerable job seekers. To prevent data leaks and protect user safety:

1. **No Raw Content Storage**: Job descriptions, private messages, chats, or applicant names are **never** stored in the data platform.
2. **Warning Codes, Not Text**: Raw warning messages (e.g., `"Recruiter uses a public email address: user@gmail.com"`) are translated into a standardized analytical code (`PUBLIC_EMAIL_DOMAIN`) before the event is recorded. The email address is discarded.
3. **Derived Metrics Only**: The platform stores numerical counts (`warning_count`, `email_count`, `phone_count`, `url_count`, `domain_count`), normalized risk scores (`0–100`), and standardized warning codes from the `WARNING_CATALOGUE`.
4. **Coarse Geographies**: Safe Journey locations are grouped into generalized municipal zones (e.g., *Johannesburg Central*) — no GPS coordinates.
5. **Zero PII**: No applicant names, ID numbers, bank credentials, or contact details ever enter the raw, staging, or warehouse layers.
6. **Non-Blocking Telemetry**: Telemetry errors can **never** interrupt or fail a live verification request.

---

## ⭐ Star Schema Data Model

The warehouse is structured as a dimensional star schema optimized for analytical queries:

### Dimension Tables
| Table | Key | Description |
| :--- | :--- | :--- |
| `dim_date` | `date_key` | Calendar date attributes (day name, month, quarter, year, is_weekend) |
| `dim_risk_category` | `risk_level` | Risk severity tiers, score ranges, action recommendations |
| `dim_input_type` | `input_type_code` | Input modalities (text only, URL, mixed) |
| `dim_warning_type` | `warning_code` | Standardized warning catalogue with category labels |
| `dim_location` | `location_zone` | Regional zones for Safe Journey telemetry |

### Fact Tables
| Table | Grain | Description |
| :--- | :--- | :--- |
| `fact_verifications` | Per verification event | Risk score, counts, warning count, processing time |
| `fact_verification_warnings` | Per warning per event | Bridge table: event ↔ warning code |
| `fact_safe_journeys` | Per journey check-in | Journey status, duration, emergency trigger |

### Governance & Audit
| Table | Description |
| :--- | :--- |
| `data_quality_metrics` | Full audit record per pipeline run (see Quality Framework below) |
| `source_validation_log` | Per-batch validation results and quarantine counts |

---

## 📊 Analytical Marts (Views)

Pre-aggregated SQL views for business intelligence:

| View | Description |
| :--- | :--- |
| `daily_verification_summary` | Daily volume, risk level counts, average score, warning counts |
| `risk_level_summary` | High / medium / low breakdown with domain threat rates |
| `warning_signal_summary` | Pareto analysis of scam trigger frequency |
| `input_type_summary` | Channel patterns and processing latencies |
| `safe_journey_summary` | Journey completions and emergency alerts by metro zone |
| `data_quality_summary` | Latest quality audit record |

---

## 🧪 Data Quality Framework

The pipeline runs automated checks across **three clearly separated dimensions**:

### 1. Raw Source Quality
*Applied to incoming records before filtering and quarantine.*

| Metric | Description |
| :--- | :--- |
| `total_received` | All events received from the source before filtering |
| `valid_records_count` | Events that passed all data contracts |
| `invalid_records_count` | Events quarantined due to contract violations |
| `duplicate_records_count` | Duplicate `event_id` records removed |
| `raw_quality_rate_pct` | `valid / total × 100` |

### 2. Warehouse Integrity
*Applied to accepted records already loaded into the warehouse.*

| Metric | Description |
| :--- | :--- |
| `warehouse_records_count` | Accepted records in warehouse fact tables |
| `constraint_violations_count` | NULL fields, invalid scores, or out-of-range values in warehouse |
| `orphan_records_count` | Facts with broken foreign key joins to dimensions |
| `warehouse_integrity_status` | `PASS` / `FAIL` |

### 3. Freshness
*Configurable threshold check on event age.*

| Metric | Description |
| :--- | :--- |
| `freshness_status` | `PASS` if latest event age ≤ `FRESHNESS_THRESHOLD_HOURS` (default: 24h) |
| `age_hours` | Hours since the most recent ingested event |

Set `FRESHNESS_THRESHOLD_HOURS` environment variable to customize the threshold.

---

## 📊 Data Contract Rules

| Dimension | Contract Rule | Enforcement |
| :--- | :--- | :--- |
| **Completeness** | `event_id`, `timestamp`, `risk_score` cannot be NULL | Quarantine before load |
| **Validity** | `risk_score` ∈ `[0, 100]`, counts `>= 0`, valid `risk_level` | Quarantine before load |
| **Uniqueness** | `event_id` must be globally unique | Duplicate removal at validation |
| **Consistency** | FK joins must resolve in `dim_date`, `dim_risk_category`, `dim_input_type` | Post-load referential check |
| **Freshness** | Latest event age ≤ `FRESHNESS_THRESHOLD_HOURS` | Configurable PASS/FAIL |

---

## 🔄 Synthetic Data Disclosure

The historical **Safe Journey** analytics in this platform uses **privacy-safe synthetic data**.

**Why?** The existing Safe Journey feature stores all session state client-side in the browser and does not transmit journey data to the backend. This is correct from a privacy perspective. Synthetic data is used to demonstrate the analytical pipeline without collecting real user location data.

**Reproducibility guarantee**: Synthetic data generation is 100% deterministic.
- Same `seed` + same parameters + same `DETERMINISTIC_BASE_TIME` = **identical dataset**
- Default: `seed=42`, `DETERMINISTIC_BASE_TIME = datetime(2026, 9, 23, 0, 0, 0, tzinfo=timezone.utc)`

---

## 🚀 Quickstart & Commands

### 1. Install Dependencies
```bash
pip install -r data-platform/requirements.txt
```

### 2. Run End-to-End Pipeline (Generates data, validates, loads, audits)
```bash
python data-platform/scripts/run_pipeline.py
```

### 3. Launch Streamlit Analytics Dashboard
```bash
streamlit run data-platform/dashboard/app.py
```

### 4. Run Full Test Suite (45 tests)
```bash
pytest data-platform/tests/ backend/ -v
```

### 5. Configure Freshness Threshold
```bash
FRESHNESS_THRESHOLD_HOURS=48 python data-platform/scripts/run_pipeline.py
```

---

## ⏱️ 5-Minute Demonstration Guide

1. **Step 1 (App)**: Run `python -m uvicorn backend.main:app` and call `POST /api/verify`. Observe the privacy-safe telemetry event in the logs — no raw text, only derived codes and counts.
2. **Step 2 (Raw Layer)**: Show `data-platform/raw/verification/YYYY-MM-DD/` with date-partitioned JSON records containing only analytical codes.
3. **Step 3 (Pipeline)**: Run `python data-platform/scripts/run_pipeline.py` to demonstrate:
   - Stage 1: Clean batch ingestion into partitioned raw landing layer
   - Stage 2: Contract validation — valid/invalid/duplicate counts, raw quality rate
   - Stage 3: Star schema dimensional transformation
   - Stage 4: Idempotent warehouse loading with referential integrity
   - Stage 5: Separated Raw Quality + Warehouse Integrity + Freshness audit
   - Stage 6: Analytical mart views built and verified
4. **Step 4 (Quality Log)**: Show `data-platform/logs/` validation summaries. Explain the difference between raw quality (source data), warehouse integrity (loaded data), and freshness.
5. **Step 5 (Dashboard)**: Launch `streamlit run data-platform/dashboard/app.py` and present:
   - Executive Overview KPIs and daily trends
   - Risk distribution and scam warning pattern analysis
   - Safe Journey regional telemetry (synthetic data — labelled clearly)
   - Data Quality & Audit tab with separated raw quality / warehouse integrity panels

---
# YouTube Video to be Submitted Later
