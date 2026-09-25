import os
from pathlib import Path

# Base Paths
PLATFORM_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = PLATFORM_DIR.parent
RAW_DATA_DIR = PLATFORM_DIR / "raw"
PROCESSED_DATA_DIR = PLATFORM_DIR / "processed"
LOGS_DIR = PLATFORM_DIR / "logs"

# Ensure runtime directories exist
for directory in [RAW_DATA_DIR, PROCESSED_DATA_DIR, LOGS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Database Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "thuso_dw")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

# Fallback SQLite DB path for local development/laptop demo without live PostgreSQL service
SQLITE_DB_PATH = PLATFORM_DIR / "warehouse" / "thuso_dw.sqlite"

# Pipeline Settings
DEFAULT_RANDOM_SEED = 42
DEFAULT_SYNTHETIC_DAYS = 30
DEFAULT_SYNTHETIC_RECORDS = 500
INVALID_RECORD_RATIO = 0.04
DUPLICATE_RECORD_RATIO = 0.03
