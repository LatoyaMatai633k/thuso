import logging
import sys
import time
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# Add platform root to sys.path
SCRIPT_DIR = Path(__file__).resolve().parent
PLATFORM_DIR = SCRIPT_DIR.parent
if str(PLATFORM_DIR) not in sys.path:
    sys.path.insert(0, str(PLATFORM_DIR))

from airflow.dags.thuso_data_pipeline_dag import (
    task_extract_and_land,
    task_validate_raw_data,
    task_transform_and_stage,
    task_load_warehouse,
    task_build_analytical_marts,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("thuso.pipeline_runner")


def run_pipeline_end_to_end():
    """
    Executes all 6 pipeline stages in sequence.
    Stage 5 (Quality) is run inline with raw_source_stats from Stage 2.
    """
    print("\n" + "=" * 70)
    print("[START] THUSO AI DATA PLATFORM END-TO-END PIPELINE")
    print("=" * 70)

    start_total = time.time()
    raw_v_summary = {}

    # Stages 1-4 run via the dag functions
    stages_1_to_4 = [
        ("1. Ingestion / Landing (clean batch)", task_extract_and_land),
        ("2. Contract Validation", task_validate_raw_data),
        ("3. Data Transformation", task_transform_and_stage),
        ("4. Warehouse Loading", task_load_warehouse),
    ]

    for idx, (stage_name, stage_func) in enumerate(stages_1_to_4, 1):
        print(f"\n>> Executing Stage {stage_name}...")
        t0 = time.time()
        try:
            result = stage_func()
            elapsed = time.time() - t0
            print(f"[OK] Stage {idx} COMPLETED in {elapsed:.2f}s -> Result: {result}")
            # Capture raw source stats from validation stage
            if idx == 2 and isinstance(result, dict) and "verification" in result:
                raw_v_summary = result.get("verification", {})
        except Exception as e:
            elapsed = time.time() - t0
            print(f"[ERROR] Stage {idx} FAILED after {elapsed:.2f}s -> Error: {e}")
            logger.exception("Pipeline failed at stage: %s", stage_name)
            raise

    # Stage 5: Quality with raw source stats
    print(f"\n>> Executing Stage 5. Data Quality Suite...")
    t0 = time.time()
    try:
        from quality.quality_checker import DataQualityChecker
        checker = DataQualityChecker()
        summary = checker.evaluate_quality_suite(
            stage_name="standalone_pipeline_run",
            raw_source_stats=raw_v_summary,
        )
        elapsed = time.time() - t0
        mr = summary["metric_record"]
        print(f"[OK] Stage 5 COMPLETED in {elapsed:.2f}s")
        print(f"     Raw Source Quality:")
        print(f"       Total Received : {mr['total_received']}")
        print(f"       Valid          : {mr['valid_records_count']}")
        print(f"       Invalid        : {mr['invalid_records_count']}")
        print(f"       Duplicates     : {mr['duplicate_records_count']}")
        print(f"       Raw Quality Rate: {mr['raw_quality_rate_pct']:.2f}%")
        print(f"     Warehouse Integrity:")
        print(f"       Warehouse Records       : {mr['warehouse_records_count']}")
        print(f"       Constraint Violations   : {mr['constraint_violations_count']}")
        print(f"       Orphan Records          : {mr['orphan_records_count']}")
        print(f"       Warehouse Integrity     : {mr['warehouse_integrity_status']}")
        print(f"     Freshness                 : {mr['freshness_status']}")
        print(f"     Overall Status            : {mr['overall_status']}")

        if mr["overall_status"] != "PASSED":
            raise ValueError(f"Data Quality Suite FAILED: {mr}")
    except Exception as e:
        elapsed = time.time() - t0
        print(f"[ERROR] Stage 5 FAILED after {elapsed:.2f}s -> Error: {e}")
        logger.exception("Pipeline failed at stage 5: Data Quality Suite")
        raise

    # Stage 6: Analytical Marts
    print(f"\n>> Executing Stage 6. Analytical Marts...")
    t0 = time.time()
    try:
        result = task_build_analytical_marts()
        elapsed = time.time() - t0
        print(f"[OK] Stage 6 COMPLETED in {elapsed:.2f}s -> Result: {result}")
    except Exception as e:
        elapsed = time.time() - t0
        print(f"[ERROR] Stage 6 FAILED after {elapsed:.2f}s -> Error: {e}")
        logger.exception("Pipeline failed at stage 6: Analytical Marts")
        raise

    total_elapsed = time.time() - start_total
    print("\n" + "=" * 70)
    print(f"[SUCCESS] PIPELINE FINISHED SUCCESSFULLY in {total_elapsed:.2f}s")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    run_pipeline_end_to_end()
