import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# Add platform root to sys.path
SCRIPT_DIR = Path(__file__).resolve().parent
PLATFORM_DIR = SCRIPT_DIR.parent
if str(PLATFORM_DIR) not in sys.path:
    sys.path.insert(0, str(PLATFORM_DIR))

from warehouse.db import db_manager


def main():
    print("Initializing Data Warehouse schema and analytical marts...")
    db_manager.initialize_schema()
    print(f"[OK] Data Warehouse initialized successfully using engine type: {db_manager.db_type.upper()}")


if __name__ == "__main__":
    main()
