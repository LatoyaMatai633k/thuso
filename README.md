# Thuso AI - Job Safety Application & Data Engineering Platform

Thuso AI is a safety-focused application that helps job seekers assess potentially fraudulent job opportunities and provides physical safety features for job interviews (Safe Journey).

This repository contains both the **core application** and the **complete Data Engineering analytical platform**.

---

##  Repository Structure

```text
thuso/
├── backend/                       # Existing FastAPI application & verification engine
│   ├── main.py                    # REST API (/api/verify, /health, /) + privacy telemetry hook
│   ├── services/                  # Cisco & auxiliary services
│   ├── requirements.txt           # Backend dependencies
│   └── test_main.py               # Backend unit test suite
│
├── frontend/                      # Existing React + Vite frontend application
│   ├── src/                       # Verification UI, Safe Journey & appointment planner
│   ├── package.json               # Frontend dependencies
│   └── vite.config.js             # Vite bundler configuration
│
├── data-platform/                 # DATA ENGINEERING PLATFORM EXTENSION
│   ├── ingestion/                 # Event emitters, synthetic data generator & landing logic
│   ├── raw/                       # Date-partitioned raw JSON landing layer
│   ├── validation/                # Data contract validators & quarantine manager
│   ├── transformations/           # Star schema transformations & cleansing engine
│   ├── warehouse/                 # Star schema DDL (schema.sql), Analytical Marts (marts.sql), DB & loaders
│   ├── quality/                   # 5-Dimensional Data Quality audit test suite
│   ├── airflow/                   # Apache Airflow DAG & standalone pipeline runner
│   ├── dashboard/                 # Streamlit analytics & executive dashboard
│   ├── tests/                     # 100% automated pytest suite for data platform
│   ├── scripts/                   # CLI entrypoints (generate_data, run_pipeline, init_db)
│   ├── config/                    # Central settings & database configuration
│   ├── docker/                    # Dockerfiles for backend, dashboard, pipeline, and airflow
│   └── README.md                  # Detailed data platform architecture guide
│
├── docker-compose.yml             # Containerized orchestration (PostgreSQL, Backend, Dashboard, Pipeline)
├── .env.example                   # Safe environment variable configuration template
└── README.md                      # Project overview and run guide
```

---

## The Data Problem & Solution

### The Problem
Thuso AI generates critical safety signals during job checks (e.g., recruitment fees, suspicious domains, private venue interviews) and Safe Journey operations. Without a structured Data Engineering platform, this data remains scattered, unvalidated, vulnerable to privacy leaks, and unavailable for analytical decision-making.

The Solution
A complete, privacy-safe Data Engineering platform that executes:
```
Collection → Ingestion → Raw Landing → Contract Validation → Dimensional Transformation → Star Schema Warehouse → Quality Audits → Analytical Marts → Interactive Streamlit Dashboard
```

---
 Privacy by Design

- **No Raw Job Text**: Job descriptions, private messages, and recruiter contact details are **never** stored.
- **Warning Codes, Not Text**: Raw warnings like `"Recruiter uses a public email address: user@gmail.com"` are translated to an analytical code (`PUBLIC_EMAIL_DOMAIN`) before the event is emitted. The email address is discarded.
- **Derived Metrics Only**: The warehouse records numerical indicators (`risk_score`, `warning_count`, `email_count`, `phone_count`, `url_count`, `domain_count`) and standardized analytical codes.
- **Generalized Locations**: Safe Journey locations are coarsened into municipal zones — no GPS coordinates.
- **Zero PII**: No applicant names, ID numbers, phone numbers, or passwords enter the analytical platform.
- **Non-Blocking Telemetry**: Telemetry failures can **never** block or fail a live verification request.

---

##  Data Warehouse Architecture (Star Schema)

### Dimension Tables
- `dim_date`: Calendar attributes (`date_key`, `full_date`, `day_name`, `quarter`, `year`, `is_weekend`)
- `dim_risk_category`: Risk tiers (`risk_level`, `score_min`, `score_max`, `action_recommendation`, `severity_tier`)
- `dim_input_type`: Submission channels (`input_type_code`, `display_name`, `description`)
- `dim_warning_type`: Catalog of threat indicators (`warning_code`, `warning_text`, `category`)
- `dim_location`: Regional municipal zones (`location_zone`, `province`, `metro_area`)

### Fact Tables
- `fact_verifications`: Primary verification events and risk metrics
- `fact_verification_warnings`: Bridge table connecting verifications to specific warning patterns
- `fact_safe_journeys`: Safe Journey telemetry, completions, and emergency alert incidents

### Governance Tables
- `data_quality_metrics`: Separated audit records: **Raw Source Quality** (received / valid / invalid / duplicate / quality rate) + **Warehouse Integrity** (constraint violations, orphan records) + **Freshness** status
- `source_validation_log`: Per-batch validation summaries

---

##  Running the Project

### Option A: Local Python Setup (Recommended for Development & Grading)

#### 1. Backend API
```bash
cd backend
pip install -r requirements.txt
python -m uvicorn main:app --reload --port 8000
```

#### 2. Frontend Web Application
```bash
cd frontend
npm install
npm run dev
```

#### 3. Data Platform Pipeline & Tests
```bash
# Install data platform requirements
pip install -r data-platform/requirements.txt

# Run full test suite (45 tests: privacy, processing time, freshness,
# raw quality separation, reproducibility, SQL errors, Airflow DAG, warning codes)
python -m pytest data-platform/tests/ backend/ -v

# Run the end-to-end Data Engineering pipeline
python data-platform/scripts/run_pipeline.py

# Launch the Streamlit Analytics Dashboard
streamlit run data-platform/dashboard/app.py
```

---

### Option B: Docker Compose Setup

Run the full stack (PostgreSQL Data Warehouse, FastAPI Backend, Streamlit Dashboard, Pipeline Runner):
```bash
docker compose up --build
```
- **FastAPI Backend**: `http://localhost:8000`
- **Streamlit Dashboard**: `http://localhost:8501`
- **PostgreSQL Warehouse**: `localhost:5432`

---

##  Key Questions the Platform Answers

1. **Verification Activity**: How many job verification checks were performed over the last 30 days?
2. **Threat Landscape**: What percentage of jobs are high, medium, or low risk?
3. **Scam Indicators**: Which warning patterns (e.g. upfront fee requests, WhatsApp-only communications) occur most frequently?
4. **Channel Patterns**: Are job seekers submitting URLs, raw text messages, or recruiter emails?
5. **Raw Source Quality**: What proportion of incoming events passed schema validation vs. quarantined? What is the raw quality rate?
5b. **Warehouse Integrity**: Do all warehouse records satisfy referential integrity and constraint rules?
5c. **Freshness**: Is the latest ingested event within the configured staleness threshold?
6. **Journey Safety**: How many Safe Journey sessions were initiated, completed, or triggered emergency alerts by municipal zone? *(synthetic data — see disclosure)*

---

##  Key Architectural Concepts for Defense & Review

- **ETL vs ELT**: Raw landing preserves source records before transformation and schema loading.
- **Idempotency**: Dimension and fact loaders avoid duplicate record creation on repeated runs.
- **Quarantine Layer**: Invalid risk scores or malformed records are isolated to audit logs without halting downstream batch pipelines.
- **Dimensional Modeling**: Separation of facts (numerical metrics) and dimensions (contextual attributes) facilitates fast aggregations.
- **Airflow Orchestration**: Explicit task dependencies (`extract >> validate >> transform >> load >> quality >> marts`) ensure data integrity.

---

##  Synthetic Data Disclosure

**Verification telemetry** in the live application is real — it is emitted by `backend/main.py` when a user submits a job for verification, using derived analytical codes and counts (no PII).

**Safe Journey historical analytics** uses **privacy-safe synthetic data**. The Safe Journey feature stores session state client-side in the browser and does not transmit user journey data to the backend, which is correct from a privacy standpoint. The synthetic dataset is used to demonstrate the analytical pipeline without collecting real user location data. This is clearly labelled in the dashboard.

**Synthetic data is 100% deterministic** — same `seed` + same parameters + same `DETERMINISTIC_BASE_TIME` = identical dataset every run.

**My WeThinkCode_ codefor my data engineering elecive is (WTC-XWC8TYK8)**

**https://youtu.be/0xS6FR-Uovs?si=JCh-yw6UQY9efC_3
This Link is for my project please watch it**
