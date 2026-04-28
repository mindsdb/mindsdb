import ast
import asyncio
import hashlib
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

import pandas as pd

try:
    from moss import (
        DocumentInfo,
        GetDocumentsOptions,
        MossClient,
        MutationOptions,
        QueryOptions,
    )
except ImportError:
    pass  # captured as import_error in __init__.py

from mindsdb.integrations.handlers.moss_handler.settings import MossHandlerConfig
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.integrations.libs.vectordatabase_handler import (
    FilterCondition,
    FilterOperator,
    TableField,
    VectorStoreHandler,
)
from mindsdb.utilities import log

logger = log.getLogger(__name__)

_executor = ThreadPoolExecutor(max_workers=4)


def _run(coro):
    future = _executor.submit(asyncio.run, coro)
    return future.result()


class MossHandler(VectorStoreHandler):
    """MindsDB handler for Moss semantic search."""

    name = "moss"

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self._client = None
        self.is_connected = False
        self._loaded_indexes: set = set()

        config = self._validate_config(name, **kwargs)
        self._project_id = config.project_id
        self._project_key = config.project_key
        self._alpha = config.alpha

    def _validate_config(self, name: str, **kwargs) -> MossHandlerConfig:
        connection_data = dict(kwargs.get("connection_data", {}))
        connection_data["vector_store"] = name
        return MossHandlerConfig(**connection_data)

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self):
        if self.is_connected:
            return self._client
        try:
            self._client = MossClient(self._project_id, self._project_key)
            self.is_connected = True
            return self._client
        except Exception as e:
            self.is_connected = False
            raise Exception(f"Error connecting to Moss: {e}")

    def disconnect(self):
        self._client = None
        self.is_connected = False
        self._loaded_indexes.clear()

    def check_connection(self) -> StatusResponse:
        response = StatusResponse(False)
        need_to_close = not self.is_connected
        try:
            client = self.connect()
            _run(client.list_indexes())
            response.success = True
        except Exception as e:
            logger.error(f"Error connecting to Moss: {e}")
            response.error_message = str(e)
        finally:
            if response.success and need_to_close:
                self.disconnect()
            if not response.success and self.is_connected:
                self.is_connected = False
        return response

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _ensure_loaded(self, index_name: str):
        if index_name not in self._loaded_indexes:
            try:
                _run(self._client.load_index(index_name))
            except Exception as e:
                if "not found" in str(e).lower():
                    raise Exception(
                        f"Index '{index_name}' not found in Moss. "
                        "Insert documents first: INSERT INTO moss_db.<index> (content) VALUES (...)"
                    ) from e
                raise
            self._loaded_indexes.add(index_name)

    def _wait_for_index_ready(self, index_name: str, timeout: int = 180):
        """Poll get_index until status is Ready."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                info = _run(self._client.get_index(index_name))
                if info.status == "Ready":
                    return
                if str(info.status).lower() == "failed":
                    raise Exception(f"Moss index build failed for '{index_name}'")
            except Exception as e:
                if "index build failed" in str(e).lower():
                    raise
                # Index not visible yet — keep polling
            time.sleep(3)
        raise Exception(f"Timed out waiting for Moss index '{index_name}' to be ready")

    def _index_exists(self, index_name: str) -> bool:
        try:
            indexes = _run(self._client.list_indexes())
            return any(idx.name == index_name for idx in (indexes or []))
        except Exception:
            return False

    def _translate_metadata_filters(self, conditions: List[FilterCondition]) -> Optional[dict]:
        if not conditions:
            return None

        prefix = TableField.METADATA.value + "."
        meta_conditions = [c for c in conditions if c.column.startswith(prefix)]
        if not meta_conditions:
            return None

        op_map = {
            FilterOperator.EQUAL: "$eq",
            FilterOperator.NOT_EQUAL: "$ne",
            FilterOperator.LESS_THAN: "$lt",
            FilterOperator.LESS_THAN_OR_EQUAL: "$lte",
            FilterOperator.GREATER_THAN: "$gt",
            FilterOperator.GREATER_THAN_OR_EQUAL: "$gte",
            FilterOperator.IN: "$in",
        }

        translated = []
        for cond in meta_conditions:
            field = cond.column.split(".", 1)[-1]
            op = op_map.get(cond.op)
            if op:
                translated.append({"field": field, "condition": {op: cond.value}})

        if not translated:
            return None
        return {"$and": translated} if len(translated) > 1 else translated[0]

    def _parse_metadata(self, raw) -> dict:
        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
            return {}
        if isinstance(raw, dict):
            return raw
        try:
            parsed = ast.literal_eval(str(raw))
            return parsed if isinstance(parsed, dict) else {}
        except (ValueError, SyntaxError):
            return {}

    # ------------------------------------------------------------------
    # VectorStoreHandler abstract methods
    # ------------------------------------------------------------------

    def select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
    ) -> pd.DataFrame:
        self.connect()
        self._ensure_loaded(table_name)

        content_filter = None
        id_filters = []
        moss_filter = self._translate_metadata_filters(conditions)

        if conditions:
            for cond in conditions:
                if cond.column == TableField.CONTENT.value:
                    content_filter = cond
                elif cond.column == TableField.ID.value:
                    if cond.op == FilterOperator.EQUAL:
                        id_filters.append(str(cond.value))
                    elif cond.op == FilterOperator.IN:
                        id_filters.extend(str(v) for v in cond.value)

        if content_filter is not None:
            query_text = content_filter.value
            if isinstance(query_text, list):
                query_text = query_text[0]

            opts = QueryOptions(
                top_k=limit or 10,
                alpha=self._alpha,
                **({"filter": moss_filter} if moss_filter else {}),
            )
            search_result = _run(self._client.query(table_name, query_text, opts))
            results = (search_result.docs if search_result is not None else []) or []

            # Moss score is 0-1 (higher = better); map to distance (lower = better)
            df = pd.DataFrame({
                TableField.ID.value: [r.id for r in results],
                TableField.CONTENT.value: [r.text for r in results],
                TableField.METADATA.value: [r.metadata for r in results],
                TableField.EMBEDDINGS.value: [None] * len(results),
                TableField.DISTANCE.value: [1.0 - (r.score or 0.0) for r in results],
            })
        else:
            get_opts = GetDocumentsOptions(doc_ids=id_filters) if id_filters else None
            docs = _run(self._client.get_docs(table_name, get_opts)) or []

            if limit is not None:
                docs = docs[offset or 0: (offset or 0) + limit]

            df = pd.DataFrame({
                TableField.ID.value: [d.id for d in docs],
                TableField.CONTENT.value: [d.text for d in docs],
                TableField.METADATA.value: [d.metadata for d in docs],
                TableField.EMBEDDINGS.value: [None] * len(docs),
            })

        if columns:
            available = [c for c in columns if c in df.columns]
            if available:
                df = df[available]

        return df

    def insert(self, table_name: str, df: pd.DataFrame) -> Response:
        self.connect()

        documents = []
        for _, row in df.iterrows():
            content = str(row.get(TableField.CONTENT.value, ""))
            doc_id = row.get(TableField.ID.value)
            doc_id_str = (
                str(doc_id)
                if doc_id is not None and not (isinstance(doc_id, float) and pd.isna(doc_id))
                else hashlib.sha256(content.encode()).hexdigest()
            )
            documents.append(DocumentInfo(
                id=doc_id_str,
                text=content,
                metadata=self._parse_metadata(row.get(TableField.METADATA.value)),
            ))

        if self._index_exists(table_name):
            _run(self._client.add_docs(table_name, documents, MutationOptions(upsert=True)))
        else:
            _run(self._client.create_index(table_name, documents))

        self._wait_for_index_ready(table_name)
        self._loaded_indexes.discard(table_name)

        return Response(RESPONSE_TYPE.OK, affected_rows=len(df))

    def delete(self, table_name: str, conditions: List[FilterCondition] = None):
        self.connect()
        if not conditions:
            raise Exception("Delete requires at least one condition")

        id_filters = []
        for cond in conditions:
            if cond.column != TableField.ID.value:
                continue
            if cond.op == FilterOperator.EQUAL:
                id_filters.append(str(cond.value))
            elif cond.op == FilterOperator.IN:
                id_filters.extend(str(v) for v in cond.value)

        if not id_filters:
            raise Exception("Moss delete only supports filtering by id")

        _run(self._client.delete_docs(table_name, id_filters))
        self._wait_for_index_ready(table_name)
        self._loaded_indexes.discard(table_name)

    def create_table(self, table_name: str, if_not_exists=True):
        pass  # index is created lazily on first insert

    def drop_table(self, table_name: str, if_exists=True):
        self.connect()
        try:
            _run(self._client.delete_index(table_name))
            self._loaded_indexes.discard(table_name)
        except Exception as e:
            if if_exists:
                return
            raise Exception(f"Failed to delete Moss index '{table_name}': {e}")

    def get_tables(self) -> Response:
        self.connect()
        indexes = _run(self._client.list_indexes()) or []
        df = pd.DataFrame({"table_name": [idx.name for idx in indexes]})
        return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=df)

    def get_columns(self, table_name: str) -> Response:
        return super().get_columns(table_name)
