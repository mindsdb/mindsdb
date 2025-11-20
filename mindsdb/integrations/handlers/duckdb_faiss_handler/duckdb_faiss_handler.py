import os
import json
from typing import List

import pandas as pd
import duckdb
from mindsdb_sql_parser.ast import (
    Select,
    Delete,
    Identifier,
    BinaryOperation,
    Constant,
    Star,
    Tuple as AstTuple,
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
from mindsdb.utilities import log
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender

from .faiss_index import FaissIndex

logger = log.getLogger(__name__)


class DuckDBFaissHandler(VectorStoreHandler, KeywordSearchBase):
    """This handler handles connection and execution of DuckDB with Faiss vector indexing."""

    name = "duckdb_faiss"

    def __init__(self, name: str, **kwargs):
        super().__init__(name=name)

        # Extract configuration
        self.connection_data = kwargs.get("connection_data", {})
        self.handler_storage = kwargs.get("handler_storage")
        self.renderer = SqlalchemyRender("postgres")

        # Storage paths
        self.persist_directory = self.connection_data.get("persist_directory")
        if self.persist_directory:
            if not os.path.exists(self.persist_directory):
                raise ValueError(f"Persist directory {self.persist_directory} does not exist")
        else:
            # Use default handler storage
            self.persist_directory = self.handler_storage.folder_get("data")
            self._use_handler_storage = True

        # DuckDB connection
        self.connection = None
        self.is_connected = False

        # Initialize storage paths
        self.duckdb_path = os.path.join(self.persist_directory, "duckdb.db")
        self.faiss_index_path = os.path.join(self.persist_directory, "faiss_index")
        self.connect()

    def connect(self) -> duckdb.DuckDBPyConnection:
        """Connect to DuckDB database."""
        if self.is_connected:
            return self.connection

        try:
            self.connection = duckdb.connect(self.duckdb_path)
            self.faiss_index = FaissIndex(self.faiss_index_path, self.connection_data)
            self.is_connected = True

            logger.info("Connected to DuckDB database")
            return self.connection

        except Exception as e:
            logger.error(f"Error connecting to DuckDB: {e}")
            raise

    def disconnect(self):
        """Close DuckDB connection."""
        if self.is_connected and self.connection:
            self.connection.close()
            self.faiss_index.close()
            self.is_connected = False

    def create_table(self, table_name: str, if_not_exists=True):
        with self.connection.cursor() as cur:
            cur.execute("CREATE SEQUENCE  IF NOT EXISTS faiss_id_sequence START 1")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS meta_data (
                    faiss_id INTEGER PRIMARY KEY DEFAULT nextval('faiss_id_sequence'), -- id in FAISS index 
                    id TEXT  NOT NULL, -- chunk id                    
                    content TEXT,
                    metadata JSON
                )
            """)

    def drop_table(self, table_name: str, if_exists=True):
        """Drop table from both DuckDB and Faiss."""
        with self.connection.cursor() as cur:
            drop_sql = f"DROP TABLE {'IF EXISTS' if if_exists else ''} meta_data"
            cur.execute(drop_sql)

        if self.faiss_index:
            self.faiss_index.drop()

    def insert(self, table_name: str, data: pd.DataFrame):
        """Insert data into both DuckDB and Faiss."""

        with self.connection.cursor() as cur:
            df_ids = cur.execute("""
                insert into meta_data (id, content, metadata) (
                    select id, content, metadata from data
                )
                RETURNING faiss_id, id
            """).fetchdf()

        data = data.merge(df_ids, on="id")

        vectors = data["embeddings"]
        ids = data["faiss_id"]

        self.faiss_index.insert(list(vectors), list(ids))
        self._sync()

    def upsert(self, table_name: str, data: pd.DataFrame):
        # delete by ids and insert
        ids = list(data['id'])
        self.delete(table_name, [FilterCondition(column='id', op=FilterOperator.IN, value=ids)])
        self.insert(table_name, data)

    def select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
    ) -> pd.DataFrame:
        """Select data with hybrid search logic."""

        vector_filter = None
        meta_filters = []
        ids = []
        for condition in conditions:
            if condition.column == "embeddings":
                vector_filter = condition
            else:
                meta_filters.append(condition)

        if vector_filter is None:
            # If only metadata in filter:
            # query duckdb only
            return self._select_from_metadata(ids, meta_filters, limit).drop("faiss_id", axis=1)

        # vector_filter is not None
        if not meta_filters:
            # If only content in filter: query faiss and attach to metadata
            return self._select_with_vector(vector_filter=vector_filter, limit=limit)

        """
        If metadata + content:
        Query faiss, use limit = 1000
        Query duckdb with `id in (...)` 
        If count of results is less than input LIMIT value
        Repeat the search with increased limit value
        Limit value for step = 1000 * 5^i  (1000, 2000, 25000, 125000 …)
        """

        df = pd.DataFrame()

        total_size = self.get_total_size()

        for i in range(10):
            batch_size = 1000 * 5**i

            # TODO implement reverse search:
            #   if batch_size > 25% of db: search metadata first and then in faiss by list of ids

            df = self._select_with_vector(vector_filter=vector_filter, meta_filters=meta_filters, limit=batch_size)
            if batch_size >= total_size or len(df) >= limit:
                break

        return df[:limit]

    def get_total_size(self):
        with self.connection.cursor() as cur:
            cur.execute("select count(1) size from meta_data")
            df = cur.fetchdf()
            return df["size"].iloc[0]

    def _select_with_vector(self, vector_filter: FilterCondition, meta_filters=None, limit=None) -> pd.DataFrame:
        embedding = vector_filter.value
        if isinstance(embedding, str):
            embedding = json.loads(embedding)

        distances, faiss_ids = self.faiss_index.search(embedding, limit or 100)

        # Fetch full data from DuckDB
        if len(faiss_ids) > 0:
            # ids = [str(idx) for idx in faiss_ids]
            meta_df = self._select_from_metadata(faiss_ids=faiss_ids, meta_filters=meta_filters)
            vector_df = pd.DataFrame({"faiss_id": faiss_ids, "distance": distances})
            return vector_df.merge(meta_df, on="faiss_id").drop("faiss_id", axis=1)

        return pd.DataFrame([], columns=["id", "content", "metadata", "distance"])

    def _select_from_metadata(self, faiss_ids=None, meta_filters=None, limit=None):
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

            if where_clause is None:
                where_clause = in_filter
            else:
                where_clause = BinaryOperation(op="AND", args=[where_clause, in_filter])

        if limit is not None:
            query.limit = Constant(limit)

        query.where = where_clause

        with self.connection.cursor() as cur:
            sql = self.renderer.get_string(query, with_failback=True)
            cur.execute(sql)
            df = cur.fetchdf()
            df["metadata"] = df["metadata"].apply(json.loads)
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

    # def update(self, table_name: str, data: pd.DataFrame, key_columns: List[str] = None) -> Response:
    #     """Update data in both DuckDB and Faiss."""
    #     # TODO implement

    # def upsert():
    #     # TODO implement

    def delete(self, table_name: str, conditions: List[FilterCondition] = None) -> Response:
        """Delete data from both DuckDB and Faiss."""

        with self.connection.cursor() as cur:
            where_clause = self._translate_filters(conditions)

            query = Select(targets=[Identifier("faiss_id")], from_table=Identifier("meta_data"), where=where_clause)
            cur.execute(self.renderer.get_string(query, with_failback=True))
            df = cur.fetchdf()
            ids = list(df["faiss_id"])

            self.faiss_index.delete_ids(ids)

            query = Delete(table=Identifier("meta_data"), where=where_clause)
            cur.execute(self.renderer.get_string(query, with_failback=True))

            self._sync()

    def _sync(self):
        """Sync the database to disk if using persistent storage"""
        self.faiss_index.dump()
        if self._use_handler_storage:
            self.handler_storage.folder_sync(self.persist_directory)

    def get_tables(self) -> Response:
        """Get list of tables."""
        data = [{"table_name": "meta_data"}]
        return Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame(data))

    def get_columns(self, table_name: str) -> Response:
        """Get table columns."""
        # TODO
        ...

    def check_connection(self) -> Response:
        """Check the connection to the database."""
        try:
            if not self.is_connected:
                self.connect()
            return StatusResponse(RESPONSE_TYPE.OK)
        except Exception as e:
            logger.error(f"Connection check failed: {e}")
            return StatusResponse(RESPONSE_TYPE.ERROR, error_message=str(e))

    def native_query(self, query: str) -> Response:
        """Execute a native SQL query."""
        try:
            with self.connection.cursor() as cur:
                cur.execute(query)
                result = cur.fetchdf()
                return Response(RESPONSE_TYPE.TABLE, data_frame=result)
        except Exception as e:
            logger.error(f"Error executing native query: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

    def __del__(self):
        """Cleanup on deletion."""
        if self.is_connected:
            self._sync()
            self.disconnect()
