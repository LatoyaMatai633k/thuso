import logging
import sys
from pathlib import Path
from typing import Any, Dict, List
import pandas as pd
from sqlalchemy import text

PLATFORM_DIR = Path(__file__).resolve().parent.parent
if str(PLATFORM_DIR) not in sys.path:
    sys.path.insert(0, str(PLATFORM_DIR))

from warehouse.db import db_manager

logger = logging.getLogger("thuso.warehouse.loader")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


class WarehouseLoader:
    def __init__(self):
        self.db = db_manager

    def load_dimension(self, table_name: str, records: List[Dict[str, Any]], primary_key: str) -> int:
        """
        Loads records into a dimension table idempotently.
        """
        if not records:
            return 0

        engine = self.db.get_engine()
        df = pd.DataFrame(records).drop_duplicates(subset=[primary_key])

        with engine.begin() as conn:
            existing_keys_query = f"SELECT {primary_key} FROM {table_name}"
            try:
                existing_df = pd.read_sql(text(existing_keys_query), conn)
                existing_keys = set(existing_df[primary_key].tolist())
            except Exception:
                existing_keys = set()

            new_df = df[~df[primary_key].isin(existing_keys)]
            if not new_df.empty:
                new_df.to_sql(table_name, conn, if_exists="append", index=False)
                logger.info("Loaded %d new records into dimension table '%s'.", len(new_df), table_name)
                return len(new_df)
            else:
                logger.debug("No new records to load into dimension '%s'.", table_name)
                return 0

    def load_fact_verifications(self, fact_records: List[Dict[str, Any]]) -> int:
        """Loads verification fact records idempotently."""
        if not fact_records:
            return 0

        engine = self.db.get_engine()
        df = pd.DataFrame(fact_records).drop_duplicates(subset=["verification_id"])

        with engine.begin() as conn:
            try:
                existing = pd.read_sql(text("SELECT verification_id FROM fact_verifications"), conn)
                existing_ids = set(existing["verification_id"].tolist())
            except Exception:
                existing_ids = set()

            new_df = df[~df["verification_id"].isin(existing_ids)]
            if not new_df.empty:
                new_df.to_sql("fact_verifications", conn, if_exists="append", index=False)
                logger.info("Loaded %d new verification facts into warehouse.", len(new_df))
                return len(new_df)
            return 0

    def load_verification_warnings_link(self, link_records: List[Dict[str, Any]]) -> int:
        """Loads verification-warning bridge table records."""
        if not link_records:
            return 0

        engine = self.db.get_engine()
        df = pd.DataFrame(link_records).drop_duplicates(subset=["verification_id", "warning_code"])

        with engine.begin() as conn:
            try:
                existing = pd.read_sql(text("SELECT verification_id, warning_code FROM fact_verification_warnings"), conn)
                existing_pairs = set(zip(existing["verification_id"], existing["warning_code"]))
            except Exception:
                existing_pairs = set()

            new_df = df[~df.apply(lambda r: (r["verification_id"], r["warning_code"]) in existing_pairs, axis=1)]
            if not new_df.empty:
                new_df.to_sql("fact_verification_warnings", conn, if_exists="append", index=False)
                logger.info("Loaded %d warning bridge links into warehouse.", len(new_df))
                return len(new_df)
            return 0

    def load_fact_safe_journeys(self, journey_records: List[Dict[str, Any]]) -> int:
        """Loads Safe Journey fact records idempotently."""
        if not journey_records:
            return 0

        engine = self.db.get_engine()
        df = pd.DataFrame(journey_records).drop_duplicates(subset=["journey_id"])

        with engine.begin() as conn:
            try:
                existing = pd.read_sql(text("SELECT journey_id FROM fact_safe_journeys"), conn)
                existing_ids = set(existing["journey_id"].tolist())
            except Exception:
                existing_ids = set()

            new_df = df[~df["journey_id"].isin(existing_ids)]
            if not new_df.empty:
                new_df.to_sql("fact_safe_journeys", conn, if_exists="append", index=False)
                logger.info("Loaded %d Safe Journey facts into warehouse.", len(new_df))
                return len(new_df)
            return 0

    def record_quality_metric(self, metric_record: Dict[str, Any]) -> None:
        """Records a data quality run audit metric entry."""
        engine = self.db.get_engine()
        df = pd.DataFrame([metric_record])
        with engine.begin() as conn:
            df.to_sql("data_quality_metrics", conn, if_exists="append", index=False)
        logger.info("Audited quality metric %s recorded in warehouse.", metric_record.get("metric_id"))
