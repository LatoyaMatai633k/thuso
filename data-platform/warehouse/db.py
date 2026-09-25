import logging
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

PLATFORM_DIR = Path(__file__).resolve().parent.parent
if str(PLATFORM_DIR) not in sys.path:
    sys.path.insert(0, str(PLATFORM_DIR))

from config.settings import (
    PLATFORM_DIR,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    SQLITE_DB_PATH,
)

logger = logging.getLogger("thuso.warehouse.db")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


class DatabaseManager:
    def __init__(self, force_sqlite: bool = False):
        self.force_sqlite = force_sqlite
        self._engine: Optional[Engine] = None
        self.db_type: str = "sqlite"

    def get_engine(self) -> Engine:
        if self._engine is not None:
            return self._engine

        if not self.force_sqlite and os.getenv("USE_POSTGRES", "false").lower() in ("true", "1"):
            pg_url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
            try:
                engine = create_engine(pg_url, pool_pre_ping=True)
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                self._engine = engine
                self.db_type = "postgresql"
                logger.info("Connected successfully to PostgreSQL at %s:%s/%s", POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB)
                return self._engine
            except Exception as e:
                logger.warning("Could not connect to PostgreSQL (%s). Falling back to SQLite warehouse.", e)

        # SQLite Warehouse Fallback
        SQLITE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        sqlite_url = f"sqlite:///{SQLITE_DB_PATH}"
        self._engine = create_engine(sqlite_url)
        self.db_type = "sqlite"
        logger.info("Initialized local SQLite Data Warehouse at: %s", SQLITE_DB_PATH)
        return self._engine

    def execute_sql_file(self, file_path: Path) -> None:
        """
        Executes DDL/DML script from a SQL file.
        Critical SQL errors are surfaced and raised rather than silently ignored.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"SQL file not found at: {file_path}")

        with open(file_path, "r", encoding="utf-8") as f:
            sql_content = f.read()

        engine = self.get_engine()
        statements = [stmt.strip() for stmt in sql_content.split(";") if stmt.strip()]

        with engine.begin() as conn:
            for idx, statement in enumerate(statements):
                try:
                    conn.execute(text(statement))
                except Exception as e:
                    # Intentionally allow idempotent DROP statements to pass if not supported by dialect
                    if statement.upper().startswith("DROP"):
                        logger.debug("Optional DROP statement note (%s): %s", e, statement[:60])
                        continue
                    logger.error("Critical SQL execution failure on statement #%d in '%s': %s\nStatement:\n%s",
                                 idx + 1, file_path.name, e, statement)
                    raise RuntimeError(f"Critical SQL execution failed in '{file_path.name}' [Statement #{idx + 1}]: {e}") from e

        logger.info("Successfully executed SQL script '%s' against %s warehouse.", file_path.name, self.db_type)

    def initialize_schema(self) -> None:
        """Executes schema.sql and marts.sql to prepare the warehouse."""
        warehouse_dir = PLATFORM_DIR / "warehouse"
        schema_path = warehouse_dir / "schema.sql"
        marts_path = warehouse_dir / "marts.sql"

        self.execute_sql_file(schema_path)
        self.execute_sql_file(marts_path)
        logger.info("Data Warehouse tables and analytical views initialized.")

    def query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Executes a SELECT query and returns a pandas DataFrame."""
        engine = self.get_engine()
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn, params=params)
        return df


# Singleton instance
db_manager = DatabaseManager()
