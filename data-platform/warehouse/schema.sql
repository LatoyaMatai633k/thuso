-- ============================================================================
-- THUSO AI DATA PLATFORM - DATA WAREHOUSE STAR SCHEMA DDL
-- Compatible with PostgreSQL and SQLite
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. DIMENSION TABLES
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date VARCHAR(10) NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(15) NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(15) NOT NULL,
    quarter INTEGER NOT NULL,
    year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_risk_category (
    risk_level VARCHAR(20) PRIMARY KEY,
    score_min INTEGER NOT NULL,
    score_max INTEGER NOT NULL,
    action_recommendation TEXT NOT NULL,
    severity_tier INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_input_type (
    input_type_code VARCHAR(30) PRIMARY KEY,
    display_name VARCHAR(50) NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS dim_warning_type (
    warning_code VARCHAR(40) PRIMARY KEY,
    warning_text TEXT NOT NULL,
    category VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_zone VARCHAR(100) PRIMARY KEY,
    province VARCHAR(50) NOT NULL,
    metro_area VARCHAR(50) NOT NULL
);

-- ----------------------------------------------------------------------------
-- 2. FACT TABLES
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_verifications (
    verification_id VARCHAR(64) PRIMARY KEY,
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    event_timestamp VARCHAR(30) NOT NULL,
    input_type VARCHAR(30) NOT NULL REFERENCES dim_input_type(input_type_code),
    user_agent_type VARCHAR(30) NOT NULL,
    risk_level VARCHAR(20) NOT NULL REFERENCES dim_risk_category(risk_level),
    risk_score INTEGER NOT NULL CHECK (risk_score >= 0 AND risk_score <= 100),
    warning_count INTEGER NOT NULL CHECK (warning_count >= 0),
    email_count INTEGER NOT NULL CHECK (email_count >= 0),
    phone_count INTEGER NOT NULL CHECK (phone_count >= 0),
    url_count INTEGER NOT NULL CHECK (url_count >= 0),
    domain_count INTEGER NOT NULL CHECK (domain_count >= 0),
    has_suspicious_domain BOOLEAN NOT NULL,
    is_shortened_url BOOLEAN NOT NULL,
    processing_time_ms REAL NOT NULL CHECK (processing_time_ms >= 0),
    event_source VARCHAR(30) NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_verification_warnings (
    verification_id VARCHAR(64) NOT NULL REFERENCES fact_verifications(verification_id) ON DELETE CASCADE,
    warning_code VARCHAR(40) NOT NULL REFERENCES dim_warning_type(warning_code),
    PRIMARY KEY (verification_id, warning_code)
);

CREATE TABLE IF NOT EXISTS fact_safe_journeys (
    journey_id VARCHAR(64) PRIMARY KEY,
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    event_timestamp VARCHAR(30) NOT NULL,
    location_zone VARCHAR(100) NOT NULL REFERENCES dim_location(location_zone),
    journey_status VARCHAR(30) NOT NULL,
    duration_minutes INTEGER NOT NULL CHECK (duration_minutes >= 0),
    emergency_trigger BOOLEAN NOT NULL,
    trusted_contacts_notified INTEGER NOT NULL CHECK (trusted_contacts_notified >= 0),
    check_in_count INTEGER NOT NULL CHECK (check_in_count >= 0),
    event_source VARCHAR(30) NOT NULL
);

-- ----------------------------------------------------------------------------
-- 3. AUDIT & QUALITY METRICS TABLE (Separates Raw Source vs Warehouse Integrity)
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS data_quality_metrics (
    metric_id VARCHAR(64) PRIMARY KEY,
    run_timestamp VARCHAR(30) NOT NULL,
    pipeline_stage VARCHAR(50) NOT NULL,
    total_received INTEGER NOT NULL,
    valid_records_count INTEGER NOT NULL,
    invalid_records_count INTEGER NOT NULL,
    duplicate_records_count INTEGER NOT NULL,
    raw_quality_rate_pct REAL NOT NULL,
    warehouse_records_count INTEGER NOT NULL,
    constraint_violations_count INTEGER NOT NULL,
    orphan_records_count INTEGER NOT NULL,
    warehouse_integrity_status VARCHAR(20) NOT NULL,
    freshness_status VARCHAR(20) NOT NULL,
    overall_status VARCHAR(20) NOT NULL
);

-- ----------------------------------------------------------------------------
-- 4. PERFORMANCE INDEXES
-- ----------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_verifications_date_key ON fact_verifications(date_key);
CREATE INDEX IF NOT EXISTS idx_verifications_risk_level ON fact_verifications(risk_level);
CREATE INDEX IF NOT EXISTS idx_verifications_input_type ON fact_verifications(input_type);
CREATE INDEX IF NOT EXISTS idx_verification_warnings_code ON fact_verification_warnings(warning_code);
CREATE INDEX IF NOT EXISTS idx_safe_journeys_date_key ON fact_safe_journeys(date_key);
CREATE INDEX IF NOT EXISTS idx_safe_journeys_location ON fact_safe_journeys(location_zone);
