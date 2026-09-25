from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add platform directory to sys.path for task execution
PLATFORM_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PLATFORM_ROOT) not in sys.path:
    sys.path.insert(0, str(PLATFORM_ROOT))

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False


def task_extract_and_land():
    """
    Extracts application / synthetic source events and lands them into raw date partitions.
    Cleans the landing directory first to ensure a clean, reproducible batch.
    """
    from ingestion.synthetic_generator import SyntheticDataGenerator
    from ingestion.ingest import ingest_batch_events

    generator = SyntheticDataGenerator(seed=42)
    verifications, journeys = generator.generate_dataset(
        num_verification_records=500,
        num_journey_records=150,
    )
    ingest_batch_events(verifications, domain="verification", clean_first=True)
    ingest_batch_events(journeys, domain="safe_journey", clean_first=True)
    return f"Raw data extracted: {len(verifications)} verification records, {len(journeys)} safe journey records."


def task_validate_raw_data():
    """
    Runs data contract validation, separates valid/invalid/duplicate records,
    and writes validation log. Returns raw source quality summary.
    """
    from validation.validator import validate_all_raw_data
    results = validate_all_raw_data()
    v_summary = results.get("verification", {}).get("summary", {})
    j_summary = results.get("safe_journey", {}).get("summary", {})
    return {
        "verification": v_summary,
        "safe_journey": j_summary,
    }


def task_transform_and_stage(**context):
    """Transforms validated records into fact and dimension datasets."""
    from validation.validator import validate_all_raw_data
    from transformations.transformer import DataTransformer

    results = validate_all_raw_data()
    v_valid = results.get("verification", {}).get("valid_records", [])
    j_valid = results.get("safe_journey", {}).get("valid_records", [])

    transformer = DataTransformer()
    v_facts, v_links, v_warnings, v_dates = transformer.transform_verifications(v_valid)
    j_facts, j_locations, j_dates = transformer.transform_safe_journeys(j_valid)
    dim_dates = transformer.build_dim_date_records(v_dates.union(j_dates))
    dim_risks = transformer.build_dim_risk_categories()
    dim_inputs = transformer.build_dim_input_types()

    return {
        "v_facts_count": len(v_facts),
        "j_facts_count": len(j_facts),
        "dim_dates_count": len(dim_dates),
        "dim_warnings_count": len(v_warnings),
        "dim_locations_count": len(j_locations),
    }


def task_load_warehouse():
    """Loads dimensions, facts, and link tables into the Data Warehouse."""
    from warehouse.db import db_manager
    from warehouse.loader import WarehouseLoader
    from validation.validator import validate_all_raw_data
    from transformations.transformer import DataTransformer

    db_manager.initialize_schema()
    loader = WarehouseLoader()
    transformer = DataTransformer()

    results = validate_all_raw_data()
    v_valid = results.get("verification", {}).get("valid_records", [])
    j_valid = results.get("safe_journey", {}).get("valid_records", [])

    v_facts, v_links, v_warnings, v_dates = transformer.transform_verifications(v_valid)
    j_facts, j_locations, j_dates = transformer.transform_safe_journeys(j_valid)
    dim_dates = transformer.build_dim_date_records(v_dates.union(j_dates))
    dim_risks = transformer.build_dim_risk_categories()
    dim_inputs = transformer.build_dim_input_types()
    dim_warning_types = transformer.build_dim_warning_types()

    loader.load_dimension("dim_date", dim_dates, "date_key")
    loader.load_dimension("dim_risk_category", dim_risks, "risk_level")
    loader.load_dimension("dim_input_type", dim_inputs, "input_type_code")
    loader.load_dimension("dim_warning_type", dim_warning_types, "warning_code")
    loader.load_dimension("dim_location", j_locations, "location_zone")

    loader.load_fact_verifications(v_facts)
    loader.load_verification_warnings_link(v_links)
    loader.load_fact_safe_journeys(j_facts)

    return f"Warehouse loading completed: {len(v_facts)} verification facts, {len(j_facts)} safe journey facts."


def task_run_quality_checks():
    """
    Executes automated data quality suite — completeness, validity, uniqueness,
    consistency, and freshness — with separated raw source quality and warehouse integrity.
    """
    from quality.quality_checker import DataQualityChecker
    from validation.validator import validate_all_raw_data

    # Re-run validation to get raw source quality stats for the quality metric record
    raw_results = validate_all_raw_data()
    raw_v_summary = raw_results.get("verification", {}).get("summary", {})

    checker = DataQualityChecker()
    summary = checker.evaluate_quality_suite(
        stage_name="airflow_automated_run",
        raw_source_stats=raw_v_summary,
    )

    if summary["metric_record"]["overall_status"] != "PASSED":
        raise ValueError(f"Data Quality Suite Failed: {summary['metric_record']}")
    return summary["metric_record"]


def task_build_analytical_marts():
    """Rebuilds analytical marts and summary views."""
    from warehouse.db import db_manager
    warehouse_dir = PLATFORM_ROOT / "warehouse"
    marts_path = warehouse_dir / "marts.sql"
    db_manager.execute_sql_file(marts_path)
    return "Analytical marts verified and active."


default_args = {
    "owner": "thuso_data_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

if AIRFLOW_AVAILABLE:
    dag = DAG(
        dag_id="thuso_data_engineering_pipeline",
        default_args=default_args,
        description="End-to-End ETL Pipeline for Thuso AI Safety Analytics Warehouse",
        schedule_interval="@daily",
        catchup=False,
        tags=["thuso", "data_platform", "safety", "analytics"],
    )

    t1_extract = PythonOperator(task_id="extract_and_land_raw", python_callable=task_extract_and_land, dag=dag)
    t2_validate = PythonOperator(task_id="validate_raw_data", python_callable=task_validate_raw_data, dag=dag)
    t3_transform = PythonOperator(task_id="transform_data", python_callable=task_transform_and_stage, dag=dag)
    t4_load = PythonOperator(task_id="load_to_warehouse", python_callable=task_load_warehouse, dag=dag)
    t5_quality = PythonOperator(task_id="run_quality_checks", python_callable=task_run_quality_checks, dag=dag)
    t6_marts = PythonOperator(task_id="build_analytical_marts", python_callable=task_build_analytical_marts, dag=dag)

    t1_extract >> t2_validate >> t3_transform >> t4_load >> t5_quality >> t6_marts
