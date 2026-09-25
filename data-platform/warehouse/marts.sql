-- ============================================================================
-- THUSO AI DATA PLATFORM - ANALYTICAL MARTS & SUMMARY VIEWS
-- Compatible with PostgreSQL and SQLite
-- ============================================================================

-- 1. Daily Verification Summary Mart
DROP VIEW IF EXISTS daily_verification_summary;
CREATE VIEW daily_verification_summary AS
SELECT
    d.full_date,
    d.day_name,
    d.is_weekend,
    COUNT(f.verification_id) AS total_verifications,
    SUM(CASE WHEN f.risk_level = 'high' THEN 1 ELSE 0 END) AS high_risk_count,
    SUM(CASE WHEN f.risk_level = 'medium' THEN 1 ELSE 0 END) AS medium_risk_count,
    SUM(CASE WHEN f.risk_level = 'low' THEN 1 ELSE 0 END) AS low_risk_count,
    ROUND(AVG(f.risk_score), 2) AS avg_risk_score,
    SUM(f.warning_count) AS total_warnings_triggered,
    SUM(CASE WHEN f.has_suspicious_domain THEN 1 ELSE 0 END) AS suspicious_domain_count,
    ROUND(AVG(f.processing_time_ms), 2) AS avg_processing_time_ms
FROM dim_date d
LEFT JOIN fact_verifications f ON d.date_key = f.date_key
GROUP BY d.full_date, d.day_name, d.is_weekend
ORDER BY d.full_date ASC;

-- 2. Risk Level Summary Mart
DROP VIEW IF EXISTS risk_level_summary;
CREATE VIEW risk_level_summary AS
SELECT
    r.risk_level,
    r.severity_tier,
    r.action_recommendation,
    COUNT(f.verification_id) AS total_verifications,
    ROUND(AVG(f.risk_score), 2) AS avg_risk_score,
    ROUND(AVG(f.warning_count), 2) AS avg_warnings_per_check,
    SUM(CASE WHEN f.has_suspicious_domain THEN 1 ELSE 0 END) AS suspicious_domains_detected,
    SUM(CASE WHEN f.is_shortened_url THEN 1 ELSE 0 END) AS shortened_urls_detected
FROM dim_risk_category r
LEFT JOIN fact_verifications f ON r.risk_level = f.risk_level
GROUP BY r.risk_level, r.severity_tier, r.action_recommendation
ORDER BY r.severity_tier ASC;

-- 3. Warning Signal Summary Mart (Pareto & Frequency analysis)
DROP VIEW IF EXISTS warning_signal_summary;
CREATE VIEW warning_signal_summary AS
SELECT
    w.warning_code,
    w.category,
    w.warning_text,
    COUNT(fvw.verification_id) AS trigger_frequency,
    ROUND(AVG(f.risk_score), 2) AS avg_associated_risk_score,
    SUM(CASE WHEN f.risk_level = 'high' THEN 1 ELSE 0 END) AS high_risk_co_occurrences
FROM dim_warning_type w
JOIN fact_verification_warnings fvw ON w.warning_code = fvw.warning_code
JOIN fact_verifications f ON fvw.verification_id = f.verification_id
GROUP BY w.warning_code, w.category, w.warning_text
ORDER BY trigger_frequency DESC;

-- 4. Input Type Summary Mart
DROP VIEW IF EXISTS input_type_summary;
CREATE VIEW input_type_summary AS
SELECT
    i.input_type_code,
    i.display_name,
    COUNT(f.verification_id) AS total_submissions,
    ROUND(AVG(f.risk_score), 2) AS avg_risk_score,
    SUM(CASE WHEN f.risk_level = 'high' THEN 1 ELSE 0 END) AS high_risk_count,
    ROUND(AVG(f.processing_time_ms), 2) AS avg_processing_time_ms
FROM dim_input_type i
LEFT JOIN fact_verifications f ON i.input_type_code = f.input_type
GROUP BY i.input_type_code, i.display_name
ORDER BY total_submissions DESC;

-- 5. Safe Journey Activity Summary Mart
DROP VIEW IF EXISTS safe_journey_summary;
CREATE VIEW safe_journey_summary AS
SELECT
    l.province,
    l.metro_area,
    l.location_zone,
    COUNT(j.journey_id) AS total_journeys_initiated,
    SUM(CASE WHEN j.journey_status = 'completed' THEN 1 ELSE 0 END) AS completed_journeys,
    SUM(CASE WHEN j.emergency_trigger THEN 1 ELSE 0 END) AS emergency_triggers,
    ROUND(AVG(j.duration_minutes), 1) AS avg_duration_minutes,
    SUM(j.trusted_contacts_notified) AS total_contacts_alerted
FROM dim_location l
LEFT JOIN fact_safe_journeys j ON l.location_zone = j.location_zone
GROUP BY l.province, l.metro_area, l.location_zone
ORDER BY total_journeys_initiated DESC;

-- 6. Data Quality Audit Summary Mart
DROP VIEW IF EXISTS data_quality_summary;
CREATE VIEW data_quality_summary AS
SELECT
    metric_id,
    run_timestamp,
    pipeline_stage,
    total_received,
    valid_records_count,
    invalid_records_count,
    duplicate_records_count,
    raw_quality_rate_pct,
    warehouse_records_count,
    constraint_violations_count,
    orphan_records_count,
    warehouse_integrity_status,
    freshness_status,
    overall_status
FROM data_quality_metrics
ORDER BY run_timestamp DESC;
