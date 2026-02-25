import os
import re
import shutil
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import List, Iterator

import pandas as pd


from mindsdb.integrations.libs.response import (
    RESPONSE_TYPE,
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.integrations.libs.vectordatabase_handler import (
    FilterCondition,
    VectorStoreHandler,
    FilterOperator,
)
from mindsdb.integrations.libs.keyword_search_base import KeywordSearchBase
from mindsdb.integrations.utilities.sql_utils import KeywordSearchArgs

from mindsdb.utilities import log
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender

from .duckdb_faiss_table import DuckDBFaissTable

logger = log.getLogger(__name__)


TABLE_CACHE_TTL_SECONDS = 60


@dataclass
class TableCacheEntry:
    table: DuckDBFaissTable
    last_used_ts: float
    in_use_count: int = 0


class DuckDBFaissHandler(VectorStoreHandler, KeywordSearchBase):
    """This handler handles connection and execution of DuckDB with Faiss vector indexing."""

    name = "duckdb_faiss"

    def __init__(self, name: str, **kwargs):
        super().__init__(name=name)
        self.single_instance = True
        self.usage_lock = False

        # Extract configuration
        self.connection_data = kwargs.get("connection_data", {})
        self.handler_storage = kwargs.get("handler_storage")
        self.renderer = SqlalchemyRender("postgres")

        # Storage paths
        self._use_handler_storage = False
        self.persist_directory = self.connection_data.get("persist_directory")
        if self.persist_directory:
            if not os.path.exists(self.persist_directory):
                raise ValueError(f"Persist directory {self.persist_directory} does not exist")
        else:
            # Use default handler storage
            self.persist_directory = self.handler_storage.folder_get("")
            self._use_handler_storage = True

        Path(self.persist_directory).mkdir(parents=True, exist_ok=True)

        self.tables_cache = {}
        self.tables_cache_lock = threading.Lock()

    def connect(self):
        """
        Handler readiness check.
        Must not open long-lived DuckDB/FAISS resources; tables are opened per operation.
        """
        if not Path(self.persist_directory).exists():
            raise ValueError(f"Persist directory {self.persist_directory} does not exist")

        self.is_connected = True
        return True

    def disconnect(self):
        with self.tables_cache_lock:
            for item in self.tables_cache.values():
                item.table.close()

            self.tables_cache = {}

    def check_connection(self) -> Response:
        """Check the connection to the database."""
        try:
            if not self.is_connected:
                self.connect()
            return StatusResponse(RESPONSE_TYPE.OK)
        except Exception as e:
            logger.error(f"Connection check failed: {e}")
            return StatusResponse(RESPONSE_TYPE.ERROR, error_message=str(e))

    def __del__(self):
        """Cleanup on deletion."""
        self.disconnect()

    # -- manage tables --

    @staticmethod
    def _validate_table_name(table_name: str) -> None:
        if table_name in (".", ".."):
            raise ValueError("Invalid table_name")
        if "/" in table_name or "\\" in table_name:
            raise ValueError("table_name must not contain path separators")
        if not re.fullmatch(r"[A-Za-z0-9_-]+", table_name):
            raise ValueError(
                "Invalid table_name: only letters, digits, '_' and '-' are allowed (no spaces, dots, or other symbols)"
            )

    def get_table_dir(self, table_name: str) -> Path:
        """
         Get folder for a table name
         Prevent path traversal by requiring the resolved path to stay within persist_directory.
        """
        root = Path(self.persist_directory).resolve()
        table_dir = (Path(self.persist_directory) / table_name).resolve()
        if table_dir == root or root not in table_dir.parents:
            raise ValueError("Invalid table_name path")
        return table_dir

    def _close_cached_table(self, table_name: str) -> None:
        entry = self.tables_cache.pop(table_name, None)
        if entry is None:
            return
        try:
            entry.table.close()
        except Exception:
            logger.exception("Failed to close cached table '%s'", table_name)

    def _close_old_tables_cache(self):
        """
        Close stale cached tables that have not been used for more than TTL.
        Tables that are currently in use are never closed by pruning.
        """
        if not self.tables_cache:
            return

        with self.tables_cache_lock:
            now_ts = time.time()
            to_close: List[str] = []
            for table_name, entry in self.tables_cache.items():
                if entry.in_use_count > 0:
                    continue
                if now_ts - entry.last_used_ts > TABLE_CACHE_TTL_SECONDS:
                    to_close.append(table_name)

            for table_name in to_close:
                self._close_cached_table(table_name)

    @contextmanager
    def open_table(self, table_name: str) -> Iterator[DuckDBFaissTable]:
        """
        Open DuckDB and Faiss resources scoped to one vector table.
        Must always be closed after use to avoid long-lived locks / RAM usage.

        If `use_cache=True` and `table.cache_required` is True, the opened table is cached
        in `self.tables_cache` and re-used across calls. Cached tables are pruned if they
        haven't been used for more than TABLE_CACHE_TTL_SECONDS.
        """
        table_dir = self.get_table_dir(table_name)
        if not table_dir.exists():
            raise ValueError(f"Table '{table_name}' does not exist")

        with self.tables_cache_lock:
            entry = self.tables_cache.get(table_name)

            if entry is not None:
                table = entry.table
            else:
                table = DuckDBFaissTable(table_name=table_name, table_dir=table_dir, handler=self).open()

                if table.cache_required:
                    entry = TableCacheEntry(table=table, last_used_ts=time.time())
                    self.tables_cache[table_name] = entry

        try:
            if entry:
                entry.in_use_count += 1

            yield table
        finally:
            if entry:
                entry.in_use_count -= 1
                entry.last_used_ts = time.time()
            else:
                table.close()

        self._close_old_tables_cache()


    def create_table(self, table_name: str, if_not_exists=True):
        self._validate_table_name(table_name)
        table_dir = self.get_table_dir(table_name)
        if table_dir.exists() and not if_not_exists:
            raise ValueError(f"Vector table '{table_name}' already exists")
        table_dir.mkdir(parents=True, exist_ok=True)

        with self.open_table(table_name) as table:
            with table.connection.cursor() as cur:
                cur.execute("CREATE SEQUENCE IF NOT EXISTS faiss_id_sequence START 1")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS meta_data (
                        faiss_id INTEGER PRIMARY KEY DEFAULT nextval('faiss_id_sequence'), -- id in FAISS index
                        id TEXT NOT NULL, -- chunk id
                        content TEXT,
                        metadata JSON
                    )
                """)

    def drop_table(self, table_name: str, if_exists=True):
        """Drop table from both DuckDB and Faiss."""
        table_dir = self.get_table_dir(table_name)

        if not table_dir.exists():
            if if_exists:
                return
            raise ValueError(f"Vector table '{table_name}' does not exist")

        with self.tables_cache_lock:
            self._close_cached_table(table_name)

        shutil.rmtree(table_dir, ignore_errors=False)

        if self._use_handler_storage:
            self.handler_storage.folder_sync(table_name)

    def get_tables(self) -> Response:
        """Get list of tables."""
        rows = []
        root = Path(self.persist_directory)
        if root.exists():
            for item in root.iterdir():
                if not item.is_dir():
                    continue
                rows.append({"table_name": item.name})
        df = pd.DataFrame(rows, columns=["table_name"])
        return Response(RESPONSE_TYPE.TABLE, data_frame=df)

    # -- table methods --

    def create_index(self, table_name: str, type: str = "ivf_file", nlist: int = None, train_count: int = None):
        with self.open_table(table_name) as table:
            table.create_index(type=type, nlist=nlist, train_count=train_count)

    def insert(self, table_name: str, data: pd.DataFrame):
        with self.open_table(table_name) as table:
            table.insert(data)

    def select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
    ) -> pd.DataFrame:
        with self.open_table(table_name) as table:
            return table.select(conditions=conditions, offset=offset, limit=limit)

    def keyword_select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
        keyword_search_args: KeywordSearchArgs = None,
    ) -> pd.DataFrame:
        with self.open_table(table_name) as table:
            return table.keyword_select(
                conditions=conditions,
                offset=offset,
                limit=limit,
                keyword_search_args=keyword_search_args,
            )

    def delete(self, table_name: str, conditions: List[FilterCondition] = None):
        """Delete data from both DuckDB and Faiss."""

        with self.open_table(table_name) as table:
            table.delete(conditions)

    def get_dimension(self, table_name: str) -> int:
        with self.open_table(table_name) as table:
            return table.get_dimension()
