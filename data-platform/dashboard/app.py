import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add platform root to sys.path
SCRIPT_DIR = Path(__file__).resolve().parent
PLATFORM_DIR = SCRIPT_DIR.parent
if str(PLATFORM_DIR) not in sys.path:
    sys.path.insert(0, str(PLATFORM_DIR))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from warehouse.db import db_manager

st.set_page_config(
    page_title="Thuso AI | Safety Data Platform",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 18px 20px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.25);
        margin-bottom: 4px;
    }
    .metric-val {
        font-size: 28px;
        font-weight: 700;
        color: #38bdf8;
    }
    .metric-label {
        font-size: 12px;
        color: #94a3b8;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .section-header {
        font-size: 15px;
        font-weight: 600;
        color: #cbd5e1;
        text-transform: uppercase;
        letter-spacing: 1px;
        border-bottom: 1px solid #334155;
        padding-bottom: 6px;
        margin-bottom: 12px;
    }
    .badge-pass {
        background-color: #166534;
        color: #86efac;
        padding: 3px 10px;
        border-radius: 12px;
        font-size: 12px;
        font-weight: 600;
    }
    .badge-fail {
        background-color: #7f1d1d;
        color: #fca5a5;
        padding: 3px 10px;
        border-radius: 12px;
        font-size: 12px;
        font-weight: 600;
    }
    .note-box {
        background: #1e293b;
        border-left: 3px solid #6366f1;
        padding: 10px 14px;
        border-radius: 4px;
        font-size: 13px;
        color: #94a3b8;
        margin: 8px 0;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=10)
def load_mart_data(query_str: str) -> pd.DataFrame:
    try:
        return db_manager.query(query_str)
    except Exception:
        return pd.DataFrame()


def ensure_db_ready():
    try:
        db_manager.initialize_schema()
    except Exception as e:
        st.sidebar.error(f"DB Init error: {e}")


ensure_db_ready()

# Sidebar
st.sidebar.title("🛡️ Thuso Data Platform")
st.sidebar.caption("Job Safety & Verification Analytics")

st.sidebar.markdown("---")
st.sidebar.subheader("Navigation")
page = st.sidebar.radio(
    "Select View:",
    [
        "📊 Executive Overview",
        "⚠️ Risk Analysis",
        "🚨 Warning Signals",
        "📱 Activity & Channels",
        "🛡️ Data Quality & Audit",
        "📍 Safe Journey Insights",
    ],
)

st.sidebar.markdown("---")
st.sidebar.caption("Warehouse Status:")
st.sidebar.info(f"Engine: **{db_manager.db_type.upper()}**")

if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# -----------------------------------------------------------------------------
# TAB 1: EXECUTIVE OVERVIEW
# -----------------------------------------------------------------------------
if page == "📊 Executive Overview":
    st.title("🛡️ Thuso AI — Safety & Verification Overview")
    st.markdown("Privacy-safe verification telemetry aggregated through the Thuso Data Engineering pipeline.")

    daily_df = load_mart_data("SELECT * FROM daily_verification_summary WHERE total_verifications > 0")
    quality_df = load_mart_data("SELECT * FROM data_quality_summary LIMIT 1")

    if daily_df.empty:
        st.warning("No verification facts found in warehouse. Run the pipeline script to populate data.")
        if st.button("🚀 Run Pipeline Now"):
            from airflow.run_pipeline import run_pipeline_end_to_end
            with st.spinner("Executing Data Engineering Pipeline..."):
                run_pipeline_end_to_end()
            st.success("Pipeline completed!")
            st.cache_data.clear()
            st.rerun()
    else:
        total_verifications = int(daily_df["total_verifications"].sum())
        total_high = int(daily_df["high_risk_count"].sum())
        total_med = int(daily_df["medium_risk_count"].sum())
        total_low = int(daily_df["low_risk_count"].sum())
        total_warnings = int(daily_df["total_warnings_triggered"].sum())

        # Quality KPIs from latest audit run
        raw_quality_pct = float(quality_df["raw_quality_rate_pct"].iloc[0]) if not quality_df.empty else None
        wh_integrity = quality_df["warehouse_integrity_status"].iloc[0] if not quality_df.empty else "N/A"
        freshness = quality_df["freshness_status"].iloc[0] if not quality_df.empty else "N/A"

        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Total Verifications</div>
                <div class="metric-val">{total_verifications:,}</div>
            </div>
            """, unsafe_allow_html=True)
        with col2:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">High Risk Flagged</div>
                <div class="metric-val" style="color:#ef4444;">{total_high:,}</div>
            </div>
            """, unsafe_allow_html=True)
        with col3:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Warnings Detected</div>
                <div class="metric-val" style="color:#a855f7;">{total_warnings:,}</div>
            </div>
            """, unsafe_allow_html=True)
        with col4:
            color = "#10b981" if raw_quality_pct and raw_quality_pct >= 90 else "#f59e0b"
            val = f"{raw_quality_pct:.1f}%" if raw_quality_pct is not None else "N/A"
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Raw Source Quality</div>
                <div class="metric-val" style="color:{color};">{val}</div>
            </div>
            """, unsafe_allow_html=True)
        with col5:
            badge_color = "#10b981" if wh_integrity == "PASS" else "#ef4444"
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Warehouse Integrity</div>
                <div class="metric-val" style="color:{badge_color};">{wh_integrity}</div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        c1, c2 = st.columns([2, 1])
        with c1:
            st.subheader("Daily Verification Volume & Risk Trajectory")
            fig_trend = px.line(
                daily_df,
                x="full_date",
                y=["total_verifications", "high_risk_count", "medium_risk_count"],
                labels={"value": "Events Count", "full_date": "Date", "variable": "Metric"},
                color_discrete_map={
                    "total_verifications": "#38bdf8",
                    "high_risk_count": "#ef4444",
                    "medium_risk_count": "#f59e0b",
                },
                template="plotly_dark",
            )
            fig_trend.update_layout(hovermode="x unified", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
            st.plotly_chart(fig_trend, use_container_width=True)

        with c2:
            st.subheader("Risk Tier Distribution")
            fig_pie = px.pie(
                values=[total_low, total_med, total_high],
                names=["Low Risk", "Medium Risk", "High Risk"],
                color=["Low Risk", "Medium Risk", "High Risk"],
                color_discrete_map={"Low Risk": "#10b981", "Medium Risk": "#f59e0b", "High Risk": "#ef4444"},
                hole=0.45,
                template="plotly_dark",
            )
            st.plotly_chart(fig_pie, use_container_width=True)

# -----------------------------------------------------------------------------
# TAB 2: RISK ANALYSIS
# -----------------------------------------------------------------------------
elif page == "⚠️ Risk Analysis":
    st.title("⚠️ Deep-Dive Risk Analysis")
    st.markdown("Detailed breakdown of risk scoring, severity tiers, and temporal risk patterns.")

    facts_df = load_mart_data("SELECT risk_score, risk_level, warning_count, has_suspicious_domain, is_shortened_url, processing_time_ms FROM fact_verifications")
    risk_summary_df = load_mart_data("SELECT * FROM risk_level_summary")

    if not facts_df.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Risk Score Distribution")
            fig_hist = px.histogram(
                facts_df, x="risk_score", nbins=25, color="risk_level",
                color_discrete_map={"low": "#10b981", "medium": "#f59e0b", "high": "#ef4444"},
                template="plotly_dark",
                labels={"risk_score": "Risk Score (0 – 100)"},
            )
            st.plotly_chart(fig_hist, use_container_width=True)

        with col2:
            st.subheader("Risk Score by Tier (Box Plot)")
            fig_box = px.box(
                facts_df, x="risk_level", y="risk_score", color="risk_level", points="all",
                color_discrete_map={"low": "#10b981", "medium": "#f59e0b", "high": "#ef4444"},
                template="plotly_dark",
            )
            st.plotly_chart(fig_box, use_container_width=True)

        st.subheader("Risk Level Summary Mart")
        st.dataframe(risk_summary_df, use_container_width=True)

# -----------------------------------------------------------------------------
# TAB 3: WARNING SIGNALS
# -----------------------------------------------------------------------------
elif page == "🚨 Warning Signals":
    st.title("🚨 Warning Signals & Scam Patterns")
    st.markdown("Frequency and category analysis of suspicious job advert indicators. Warning signals are stored as derived analytical codes — no raw PII.")

    warn_df = load_mart_data("SELECT * FROM warning_signal_summary")

    if not warn_df.empty:
        c1, c2 = st.columns([2, 1])
        with c1:
            st.subheader("Top Warning Signals (Pareto Analysis)")
            fig_bar = px.bar(
                warn_df.head(10), x="trigger_frequency", y="warning_text", orientation="h",
                color="category", template="plotly_dark",
                labels={"trigger_frequency": "Occurrences", "warning_text": "Warning Category"},
            )
            fig_bar.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_bar, use_container_width=True)

        with c2:
            st.subheader("Category Proportions")
            cat_counts = warn_df.groupby("category")["trigger_frequency"].sum().reset_index()
            fig_cat = px.pie(cat_counts, values="trigger_frequency", names="category", template="plotly_dark", hole=0.4)
            st.plotly_chart(fig_cat, use_container_width=True)

        st.subheader("Warning Signal Mart Table")
        st.caption("⚠️ 'warning_text' shows the analytical category label, not any raw user input or PII.")
        st.dataframe(warn_df, use_container_width=True)

# -----------------------------------------------------------------------------
# TAB 4: ACTIVITY & CHANNELS
# -----------------------------------------------------------------------------
elif page == "📱 Activity & Channels":
    st.title("📱 Submission Channels & Telemetry")
    st.markdown("Distribution of input modalities and processing latencies.")

    input_df = load_mart_data("SELECT * FROM input_type_summary")
    facts_df = load_mart_data("SELECT input_type, user_agent_type, processing_time_ms, risk_score FROM fact_verifications")

    if not input_df.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Submissions by Input Type")
            fig_inp = px.bar(
                input_df, x="display_name", y="total_submissions", color="display_name",
                template="plotly_dark",
                labels={"display_name": "Input Type", "total_submissions": "Count"},
            )
            st.plotly_chart(fig_inp, use_container_width=True)

        with col2:
            st.subheader("Client Device Types")
            ua_counts = facts_df["user_agent_type"].value_counts().reset_index()
            ua_counts.columns = ["user_agent_type", "count"]
            fig_ua = px.pie(ua_counts, values="count", names="user_agent_type", template="plotly_dark", hole=0.4)
            st.plotly_chart(fig_ua, use_container_width=True)

        st.subheader("Channel Comparison Mart")
        st.dataframe(input_df, use_container_width=True)

# -----------------------------------------------------------------------------
# TAB 5: DATA QUALITY & AUDIT
# -----------------------------------------------------------------------------
elif page == "🛡️ Data Quality & Audit":
    st.title("🛡️ Data Quality & Pipeline Governance")
    st.markdown("Audited metrics across the full quality framework.")

    quality_df = load_mart_data("SELECT * FROM data_quality_summary")

    if not quality_df.empty:
        latest = quality_df.iloc[0]

        # ---- Raw Source Quality Section ----
        st.markdown('<div class="section-header">📥 Raw Data Quality (Source Records Before Filtering)</div>', unsafe_allow_html=True)
        st.markdown("""
        <div class="note-box">
        Raw data quality measures the quality of <strong>incoming records before filtering and quarantine</strong>.
        It reflects the proportion of source events that passed all data contract checks.
        </div>
        """, unsafe_allow_html=True)

        r1, r2, r3, r4, r5 = st.columns(5)
        with r1:
            st.metric("Total Received", f"{int(latest['total_received']):,}")
        with r2:
            st.metric("Valid Records", f"{int(latest['valid_records_count']):,}")
        with r3:
            st.metric("Invalid / Quarantined", f"{int(latest['invalid_records_count']):,}")
        with r4:
            st.metric("Duplicates Removed", f"{int(latest['duplicate_records_count']):,}")
        with r5:
            rq = float(latest["raw_quality_rate_pct"])
            color = "normal" if rq >= 90 else "inverse"
            st.metric("Raw Quality Rate", f"{rq:.2f}%")

        st.markdown("<br>", unsafe_allow_html=True)

        # ---- Warehouse Integrity Section ----
        st.markdown('<div class="section-header">🏛️ Warehouse Integrity (Accepted Records in Warehouse)</div>', unsafe_allow_html=True)
        st.markdown("""
        <div class="note-box">
        Warehouse integrity measures whether <strong>accepted records satisfy warehouse constraints and quality rules</strong>.
        This is separate from source-data quality — a clean warehouse does not imply the source data was 100% clean.
        </div>
        """, unsafe_allow_html=True)

        w1, w2, w3, w4 = st.columns(4)
        with w1:
            st.metric("Warehouse Records", f"{int(latest['warehouse_records_count']):,}")
        with w2:
            st.metric("Constraint Violations", f"{int(latest['constraint_violations_count']):,}")
        with w3:
            st.metric("Orphan Records", f"{int(latest['orphan_records_count']):,}")
        with w4:
            wh_status = str(latest["warehouse_integrity_status"])
            badge = "badge-pass" if wh_status == "PASS" else "badge-fail"
            st.markdown(f'<div style="padding-top:28px;"><span class="{badge}">{wh_status}</span></div>', unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # ---- Freshness Section ----
        st.markdown('<div class="section-header">⏱️ Data Freshness</div>', unsafe_allow_html=True)
        f_status = str(latest["freshness_status"])
        f_badge = "badge-pass" if f_status == "PASS" else "badge-fail"
        st.markdown(f'Freshness Status: <span class="{f_badge}">{f_status}</span>', unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # ---- Audit Trail ----
        st.subheader("Full Quality Audit Trail")
        st.dataframe(quality_df, use_container_width=True)

        st.subheader("Data Contract Enforcement Rules")
        st.markdown("""
        | Dimension | Contract Rule | Enforcement |
        | :--- | :--- | :--- |
        | **Completeness** | `event_id`, `timestamp`, `risk_score` cannot be NULL | Quarantined before warehouse load |
        | **Validity** | `risk_score` in `[0, 100]`, counts >= 0, valid `risk_level` | Quarantined before warehouse load |
        | **Uniqueness** | `event_id` must be unique across all partitions | Duplicate events removed at validation |
        | **Consistency** | Foreign keys must resolve in `dim_date`, `dim_risk_category`, `dim_input_type` | Referential integrity verified post-load |
        | **Freshness** | Latest event age must be within `FRESHNESS_THRESHOLD_HOURS` (default: 24h) | PASS/FAIL against configurable threshold |
        """)
    else:
        st.info("No quality audit records found. Run the pipeline to generate quality metrics.")
        if st.button("🚀 Run Pipeline"):
            from airflow.run_pipeline import run_pipeline_end_to_end
            with st.spinner("Running pipeline..."):
                run_pipeline_end_to_end()
            st.cache_data.clear()
            st.rerun()

# -----------------------------------------------------------------------------
# TAB 6: SAFE JOURNEY INSIGHTS
# -----------------------------------------------------------------------------
elif page == "📍 Safe Journey Insights":
    st.title("📍 Safe Journey Safety Analytics")
    st.markdown("Aggregated telemetry for Safe Journey check-ins and emergency alerts by region.")

    st.markdown("""
    <div class="note-box">
    ℹ️ <strong>Synthetic Data Disclosure:</strong> Safe Journey historical analytics currently uses
    privacy-safe <strong>synthetic events</strong>. The existing Safe Journey feature stores session
    state client-side in the browser. This synthetic dataset demonstrates the analytical pipeline
    without collecting real user location or journey data.
    </div>
    """, unsafe_allow_html=True)

    journey_df = load_mart_data("SELECT * FROM safe_journey_summary")

    if not journey_df.empty:
        tot_journeys = int(journey_df["total_journeys_initiated"].sum())
        tot_alerts = int(journey_df["emergency_triggers"].sum())
        tot_completed = int(journey_df["completed_journeys"].sum())

        j1, j2, j3 = st.columns(3)
        with j1:
            st.metric("Total Journeys Initiated", f"{tot_journeys:,}")
        with j2:
            st.metric("Successfully Completed", f"{tot_completed:,}")
        with j3:
            st.metric("Emergency Alerts Triggered", f"{tot_alerts:,}", delta=f"{tot_alerts} incidents", delta_color="inverse")

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Journeys by Metro Area")
            metro_df = journey_df.groupby("metro_area")["total_journeys_initiated"].sum().reset_index()
            fig_metro = px.bar(metro_df, x="metro_area", y="total_journeys_initiated", color="metro_area", template="plotly_dark")
            st.plotly_chart(fig_metro, use_container_width=True)

        with col2:
            st.subheader("Emergency Triggers by Location Zone")
            alert_df = journey_df[journey_df["emergency_triggers"] > 0]
            if not alert_df.empty:
                fig_alert = px.bar(alert_df, x="location_zone", y="emergency_triggers", color="province", template="plotly_dark")
                st.plotly_chart(fig_alert, use_container_width=True)
            else:
                st.info("No emergency triggers recorded in this dataset.")

        st.subheader("Safe Journey Summary Mart")
        st.caption("⚠️ Location zones are coarsened municipal regions — no precise GPS or personal location data is stored.")
        st.dataframe(journey_df, use_container_width=True)
