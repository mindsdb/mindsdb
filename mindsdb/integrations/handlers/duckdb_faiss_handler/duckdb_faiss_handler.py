import os
import re
import shutil
from contextlib import contextmanager
from pathlib import Path
from typing import List, Iterator, Tuple

import pandas as pd
import orjson
import duckdb
from mindsdb_sql_parser.ast import (
    Select,
    Delete,
    Identifier,
    BinaryOperation,
    Constant,
    NullConstant,
    Star,
    Tuple as AstTuple,
    Function,
    TypeCast,
)

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

from .faiss_index import FaissIVFIndex

logger = log.getLogger(__name__)


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
        self._handler_storage_folder_name = None
        self.persist_directory = self.connection_data.get("persist_directory")
        if self.persist_directory:
            if not os.path.exists(self.persist_directory):
                raise ValueError(f"Persist directory {self.persist_directory} does not exist")
        else:
            # Use default handler storage
            self._handler_storage_folder_name = "data"
            self.persist_directory = self.handler_storage.folder_get(self._handler_storage_folder_name)
            self._use_handler_storage = True

        Path(self.persist_directory).mkdir(parents=True, exist_ok=True)

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

    def _table_dir(self, table_name: str) -> Path:
        """
        Map a table name to a folder under persist_directory.

        Note: do not call _validate_table_name() here (per requirement). Instead, prevent
        path traversal by requiring the resolved path to stay within persist_directory.
        """
        root = Path(self.persist_directory).resolve()
        table_dir = (Path(self.persist_directory) / table_name).resolve()
        if table_dir == root or root not in table_dir.parents:
            raise ValueError("Invalid table_name path")
        return table_dir

    @contextmanager
    def _open_table(
        self, table_name: str
    ) -> Iterator[Tuple[duckdb.DuckDBPyConnection, FaissIVFIndex]]:
        """
        Open DuckDB and Faiss resources scoped to one vector table.
        Must always be closed after use to avoid long-lived locks / RAM usage.
        """
        table_dir = self._table_dir(table_name)
        if not table_dir.exists():
            raise ValueError(f"Vector table '{table_name}' does not exist")

        duckdb_path = table_dir / "duckdb.db"
        connection = duckdb.connect(str(duckdb_path))
        faiss_index = FaissIVFIndex(str(table_dir), self.connection_data)
        try:
            yield connection, faiss_index
        finally:
            try:
                faiss_index.close()
            except Exception:
                logger.exception("Failed to close FAISS index")
            try:
                connection.close()
            except Exception:
                logger.exception("Failed to close DuckDB connection")

    def connect(self):
        """
        Handler readiness check.
        Must not open long-lived DuckDB/FAISS resources; tables are opened per operation.
        """
        self.is_connected = True
        return True

    def disconnect(self):
        self.is_connected = False

    @staticmethod
    def _is_kw_index_enabled(connection: duckdb.DuckDBPyConnection) -> bool:
        with connection.cursor() as cur:
            df = cur.execute(
                "SELECT * FROM information_schema.schemata WHERE schema_name = 'fts_main_meta_data'"
            ).fetchdf()
            return len(df) > 0

    def _create_kw_index(self, connection: duckdb.DuckDBPyConnection):
        with connection.cursor() as cur:
            cur.execute("PRAGMA create_fts_index('meta_data', 'id', 'content')")

    def _drop_kw_index(self, connection: duckdb.DuckDBPyConnection):
        with connection.cursor() as cur:
            cur.execute("pragma drop_fts_index('meta_data')")

    def create_table(self, table_name: str, if_not_exists=True):
        self._validate_table_name(table_name)
        table_dir = self._table_dir(table_name)
        if table_dir.exists() and not if_not_exists:
            raise ValueError(f"Vector table '{table_name}' already exists")
        table_dir.mkdir(parents=True, exist_ok=True)

        with self._open_table(table_name) as (connection, _faiss_index):
            with connection.cursor() as cur:
                cur.execute("CREATE SEQUENCE IF NOT EXISTS faiss_id_sequence START 1")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS meta_data (
                        faiss_id INTEGER PRIMARY KEY DEFAULT nextval('faiss_id_sequence'), -- id in FAISS index
                        id TEXT NOT NULL, -- chunk id
                        content TEXT,
                        metadata JSON
                    )
                """)
            self._sync(table_name, connection=connection, faiss_index=_faiss_index)

    def drop_table(self, table_name: str, if_exists=True):
        """Drop table from both DuckDB and Faiss."""
        table_dir = self._table_dir(table_name)
        if if_exists and not table_dir.exists():
            return
        if not if_exists and not table_dir.exists():
            raise ValueError(f"Vector table '{table_name}' does not exist")

        with self._open_table(table_name) as (connection, faiss_index):

            with connection.cursor() as cur:
                drop_sql = f"DROP TABLE {'IF EXISTS' if if_exists else ''} meta_data"
                cur.execute(drop_sql)
            faiss_index.drop()
            self._sync(table_name, connection=connection, faiss_index=faiss_index)
        try:
            shutil.rmtree(table_dir, ignore_errors=False)
        except FileNotFoundError:
            pass
        except Exception:
            logger.exception("Failed to remove vector table directory: %s", table_dir)

    def create_index(self, table_name: str, type: str = "ivf_file", nlist: int = None, train_count: int = None):
        if type not in ("ivf", "ivf_file"):
            raise NotImplementedError("Only ivf or ivf_file indexes are supported")

        with self._open_table(table_name) as (connection, faiss_index):
            faiss_index.create_index(type, nlist=nlist, train_count=train_count)
            self._sync(table_name, connection=connection, faiss_index=faiss_index)

    def insert(self, table_name: str, data: pd.DataFrame):
        """Insert data into both DuckDB and Faiss."""

        with self._open_table(table_name) as (connection, faiss_index):
            if self._is_kw_index_enabled(connection):
                # drop index, it will be created before a first keyword search
                self._drop_kw_index(connection)

            with connection.cursor() as cur:
                df_ids = cur.execute("""
                    insert into meta_data (id, content, metadata) (
                        select id, content, metadata from data
                    )
                    RETURNING faiss_id, id
                """).fetchdf()

            data = data.merge(df_ids, on="id")

            vectors = data["embeddings"]
            ids = data["faiss_id"]

            faiss_index.insert(list(vectors), list(ids))
            self._sync(table_name, connection=connection, faiss_index=faiss_index)

    # def upsert(self, table_name: str, data: pd.DataFrame):
    #     # delete by ids and insert
    #     ids = list(data['id'])
    #     self.delete(table_name, [FilterCondition(column='id', op=FilterOperator.IN, value=ids)])
    #     self.insert(table_name, data)

    def select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
    ) -> pd.DataFrame:
        """Select data with hybrid search logic."""

        with self._open_table(table_name) as (connection, faiss_index):
            vector_filter = None
            meta_filters = []
            if conditions is None:
                conditions = []
            for condition in conditions:
                if condition.column == "embeddings":
                    vector_filter = condition
                else:
                    meta_filters.append(condition)

            if vector_filter is None:
                # If only metadata in filter:
                # query duckdb only
                return (
                    self._select_from_metadata(connection=connection, meta_filters=meta_filters, limit=limit)
                    .drop("faiss_id", axis=1)
                )

            # vector_filter is not None
            if not meta_filters:
                # If only content in filter: query faiss and attach to metadata
                return self._select_with_vector(
                    connection=connection, faiss_index=faiss_index, vector_filter=vector_filter, limit=limit
                )

            """
            If metadata + content:
            Query faiss, use limit = 1000
            Query duckdb with `id in (...)`
            If count of results is less than input LIMIT value
            Repeat the search with increased limit value
            Limit value for step = 1000 * 5^i  (1000, 2000, 25000, 125000 …)
            """

            df = pd.DataFrame()
            total_size = self._get_total_size(connection)

            for i in range(10):
                batch_size = 1000 * 5**i

                # TODO implement reverse search:
                #   if batch_size > 25% of db: search metadata first and then in faiss by list of ids

                df = self._select_with_vector(
                    connection=connection,
                    faiss_index=faiss_index,
                    vector_filter=vector_filter,
                    meta_filters=meta_filters,
                    limit=batch_size,
                )
                if batch_size >= total_size or (limit is not None and len(df) >= limit):
                    break

            return df if limit is None else df[:limit]

    def keyword_select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
        keyword_search_args: KeywordSearchArgs = None,
    ) -> pd.DataFrame:
        with self._open_table(table_name) as (connection, faiss_index):
            if not self._is_kw_index_enabled(connection):
                # keyword search is used for first time: create index
                self._create_kw_index(connection)
                self._sync(table_name, connection=connection, faiss_index=faiss_index)

            with connection.cursor() as cur:
                where_clause = self._translate_filters(conditions)

                score = Function(
                    namespace="fts_main_meta_data",
                    op="match_bm25",
                    args=[
                        Identifier("id"),
                        Constant(keyword_search_args.query),
                        BinaryOperation(op=":=", args=[Identifier("fields"), Constant(keyword_search_args.column)]),
                    ],
                )

                no_emtpy_score = BinaryOperation(op="is not", args=[score, NullConstant()])
                if where_clause:
                    where_clause = BinaryOperation(op="and", args=[where_clause, no_emtpy_score])
                else:
                    where_clause = no_emtpy_score

                query = Select(
                    targets=[Star(), BinaryOperation(op="-", args=[Constant(1), score], alias=Identifier("distance"))],
                    from_table=Identifier("meta_data"),
                    where=where_clause,
                )

                sql = self.renderer.get_string(query, with_failback=True)
                cur.execute(sql)
                df = cur.fetchdf()
                df["metadata"] = df["metadata"].apply(orjson.loads)
                return df

    @staticmethod
    def _get_total_size(connection: duckdb.DuckDBPyConnection) -> int:
        with connection.cursor() as cur:
            cur.execute("select count(1) size from meta_data")
            df = cur.fetchdf()
            return int(df["size"].iloc[0])

    def _select_with_vector(
        self,
        connection: duckdb.DuckDBPyConnection,
        faiss_index: FaissIVFIndex,
        vector_filter: FilterCondition,
        meta_filters=None,
        limit=None,
    ) -> pd.DataFrame:
        embedding = vector_filter.value
        if isinstance(embedding, str):
            embedding = orjson.loads(embedding)

        distances, faiss_ids = faiss_index.search(embedding, limit or 100)

        # Fetch full data from DuckDB
        if len(faiss_ids) > 0:
            # ids = [str(idx) for idx in faiss_ids]
            meta_df = self._select_from_metadata(connection=connection, faiss_ids=faiss_ids, meta_filters=meta_filters)
            vector_df = pd.DataFrame({"faiss_id": faiss_ids, "distance": distances})
            return vector_df.merge(meta_df, on="faiss_id").drop("faiss_id", axis=1).sort_values(by="distance")

        return pd.DataFrame([], columns=["id", "content", "metadata", "distance"])

    def _select_from_metadata(self, connection: duckdb.DuckDBPyConnection, faiss_ids=None, meta_filters=None, limit=None):
        query = Select(
            targets=[Star()],
            from_table=Identifier("meta_data"),
        )

        where_clause = self._translate_filters(meta_filters)

        if faiss_ids:
            # TODO what if ids list is too long - split search into batches
            in_filter = BinaryOperation(
                op="IN", args=[Identifier("faiss_id"), AstTuple([Constant(i) for i in faiss_ids])]
            )
            # split into chunks
            chunk_size = 10000
            if len(faiss_ids) > chunk_size:
                dfs = []
                chunk = 0
                total = 0
                while chunk * chunk_size < len(faiss_ids):
                    # create results with partition
                    ids = faiss_ids[chunk * chunk_size : (chunk + 1) * chunk_size]
                    chunk += 1
                    df = self._select_from_metadata(connection=connection, faiss_ids=ids, meta_filters=meta_filters, limit=limit)
                    total += len(df)
                    if limit is not None and limit <= total:
                        # cut the extra from the end
                        df = df[: -(total - limit)]
                        dfs.append(df)
                        break
                    if len(df) > 0:
                        dfs.append(df)
                if len(dfs) == 0:
                    return pd.DataFrame([], columns=["faiss_id", "id", "content", "metadata"])
                return pd.concat(dfs)

            if where_clause is None:
                where_clause = in_filter
            else:
                where_clause = BinaryOperation(op="AND", args=[where_clause, in_filter])

        if limit is not None:
            query.limit = Constant(limit)

        query.where = where_clause

        with connection.cursor() as cur:
            sql = self.renderer.get_string(query, with_failback=True)
            cur.execute(sql)
            df = cur.fetchdf()
            df["metadata"] = df["metadata"].apply(orjson.loads)
            return df

    def _translate_filters(self, meta_filters):
        if not meta_filters:
            return None

        where_clause = None
        for item in meta_filters:
            parts = item.column.split(".")
            key = Identifier(parts[0])

            # converts 'col.el1.el2' to col->'el1'->>'el2'
            if len(parts) > 1:
                # intermediate elements
                for el in parts[1:-1]:
                    key = BinaryOperation(op="->", args=[key, Constant(el)])

                # last element
                key = BinaryOperation(op="->>", args=[key, Constant(parts[-1])])

            is_orig_id = item.column == "metadata._original_doc_id"

            type_cast = None
            value = item.value

            if isinstance(value, list) and len(value) > 0 and item.op in (FilterOperator.IN, FilterOperator.NOT_IN):
                if is_orig_id:
                    # convert to str
                    item.value = [str(i) for i in value]
                value = item.value[0]
            elif is_orig_id:
                if not isinstance(value, str):
                    value = item.value = str(item.value)

            if isinstance(value, int):
                type_cast = "int"
            elif isinstance(value, float):
                type_cast = "float"

            if type_cast is not None:
                key = TypeCast(type_cast, key)

            if item.op in (FilterOperator.NOT_IN, FilterOperator.IN):
                values = [Constant(i) for i in item.value]
                value = AstTuple(values)
            else:
                value = Constant(item.value)

            condition = BinaryOperation(op=item.op.value, args=[key, value])

            if where_clause is None:
                where_clause = condition
            else:
                where_clause = BinaryOperation(op="AND", args=[where_clause, condition])
        return where_clause

    def delete(self, table_name: str, conditions: List[FilterCondition] = None) -> Response:
        """Delete data from both DuckDB and Faiss."""

        with self._open_table(table_name) as (connection, faiss_index):
            with connection.cursor() as cur:
                where_clause = self._translate_filters(conditions)

                query = Select(targets=[Identifier("faiss_id")], from_table=Identifier("meta_data"), where=where_clause)
                cur.execute(self.renderer.get_string(query, with_failback=True))
                df = cur.fetchdf()
                ids = list(df["faiss_id"])

                faiss_index.delete_ids(ids)

                query = Delete(table=Identifier("meta_data"), where=where_clause)
                cur.execute(self.renderer.get_string(query, with_failback=True))

                self._sync(table_name, connection=connection, faiss_index=faiss_index)

    def get_dimension(self, table_name: str) -> int:
        with self._open_table(table_name) as (_connection, faiss_index):
            if faiss_index and faiss_index.index is not None:
                return faiss_index.dim

    def _sync(self, table_name: str, connection: duckdb.DuckDBPyConnection, faiss_index: FaissIVFIndex):
        """Sync FAISS index and handler storage (if used)."""
        try:
            faiss_index.dump()
        except Exception:
            logger.exception("Failed to dump FAISS index")
        if self._use_handler_storage:
            # Sync the whole handler data folder (contains all table subfolders).
            self.handler_storage.folder_sync(self._handler_storage_folder_name or "data")

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

    def check_connection(self) -> Response:
        """Check the connection to the database."""
        try:
            self.connect()
            return StatusResponse(RESPONSE_TYPE.OK)
        except Exception as e:
            logger.error(f"Connection check failed: {e}")
            return StatusResponse(RESPONSE_TYPE.ERROR, error_message=str(e))

    def native_query(self, query: str) -> Response:
        """Execute a native SQL query."""
        try:
            tables = self.get_tables().data_frame["table_name"].tolist()
            if len(tables) == 1:
                table_name = tables[0]
            else:
                return Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=(
                        "native_query is ambiguous for duckdb_faiss with multiple vector tables. "
                        "Create exactly one table or use the vector-table APIs."
                    ),
                )
            with self._open_table(table_name) as (connection, _faiss_index):
                with connection.cursor() as cur:
                    cur.execute(query)
                    result = cur.fetchdf()
                    return Response(RESPONSE_TYPE.TABLE, data_frame=result)
        except Exception as e:
            logger.error(f"Error executing native query: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

    def __del__(self):
        """Cleanup on deletion."""
        self.disconnect()
