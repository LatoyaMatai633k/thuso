import json
import logging
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

PLATFORM_DIR = Path(__file__).resolve().parent.parent
if str(PLATFORM_DIR) not in sys.path:
    sys.path.insert(0, str(PLATFORM_DIR))

from config.settings import RAW_DATA_DIR

logger = logging.getLogger("thuso.ingestion")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


def clean_raw_landing(domain: Optional[str] = None) -> None:
    """Cleans raw landing directory to ensure repeatable pipeline demonstrations."""
    if domain:
        target_dir = RAW_DATA_DIR / domain
        if target_dir.exists():
            shutil.rmtree(target_dir)
            target_dir.mkdir(parents=True, exist_ok=True)
            logger.info("Cleaned raw landing partition directory for domain: %s", domain)
    else:
        if RAW_DATA_DIR.exists():
            shutil.rmtree(RAW_DATA_DIR)
            RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
            logger.info("Cleaned full raw landing layer at: %s", RAW_DATA_DIR)


def get_partition_path(domain: str, timestamp_str: str) -> Path:
    """Computes partitioned date folder e.g., raw/verification/2026-09-20/"""
    try:
        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        date_folder = dt.strftime("%Y-%m-%d")
    except Exception:
        date_folder = "unpartitioned_or_malformed"

    partition_dir = RAW_DATA_DIR / domain / date_folder
    partition_dir.mkdir(parents=True, exist_ok=True)
    return partition_dir


def ingest_single_event(event: Dict[str, Any], domain: Optional[str] = None) -> Path:
    """
    Lands a single privacy-safe event directly into the raw layer partition.
    """
    if domain is None:
        domain = "verification" if event.get("event_type") in ("job_verification", "verification") else "safe_journey"

    ts = event.get("timestamp") or event.get("event_timestamp") or datetime.now(timezone.utc).isoformat()
    partition_dir = get_partition_path(domain, str(ts))

    event_id = event.get("event_id") or f"unknown_{int(datetime.now(timezone.utc).timestamp()*1000)}"
    file_path = partition_dir / f"{event_id}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(event, f, indent=2)

    return file_path


def ingest_batch_events(events: List[Dict[str, Any]], domain: str, clean_first: bool = False) -> Dict[str, int]:
    """
    Ingests a batch of raw events, partitioning them by calendar date.
    Optionally cleans domain landing directory first for clean, repeatable runs.
    """
    if clean_first:
        clean_raw_landing(domain)

    counts_by_partition: Dict[str, int] = {}

    for idx, event in enumerate(events):
        ts = event.get("timestamp") or event.get("event_timestamp") or datetime.now(timezone.utc).isoformat()
        partition_dir = get_partition_path(domain, str(ts))
        partition_key = str(partition_dir.relative_to(RAW_DATA_DIR))

        event_id = event.get("event_id") or f"gen_{idx}_{int(datetime.now(timezone.utc).timestamp())}"
        file_path = partition_dir / f"{event_id}.json"

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(event, f, indent=2)

        counts_by_partition[partition_key] = counts_by_partition.get(partition_key, 0) + 1

    total_ingested = sum(counts_by_partition.values())
    logger.info("Successfully ingested %d raw events into %d partition folders for domain '%s'",
                total_ingested, len(counts_by_partition), domain)
    return counts_by_partition
