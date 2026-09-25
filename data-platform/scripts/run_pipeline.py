import sys
from pathlib import Path

# Add platform root to sys.path
SCRIPT_DIR = Path(__file__).resolve().parent
PLATFORM_DIR = SCRIPT_DIR.parent
if str(PLATFORM_DIR) not in sys.path:
    sys.path.insert(0, str(PLATFORM_DIR))

from airflow.run_pipeline import run_pipeline_end_to_end

if __name__ == "__main__":
    run_pipeline_end_to_end()
