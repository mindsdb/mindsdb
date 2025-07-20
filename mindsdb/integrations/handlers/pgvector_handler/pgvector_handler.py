import os
import json
from typing import Dict, List, Literal, Tuple
from urllib.parse import urlparse

import pandas as pd
import psycopg
from mindsdb_sql_parser.ast import (
    Parameter,
    Identifier,
    BinaryOperation,
    Tuple as AstTuple,
    Constant,
    Select,
    OrderBy,
    TypeCast,
    Delete,
    Update,
    Function,
)
from pgvector.psycopg import register_vector

from mindsdb.integrations.handlers.postgres_handler.postgres_handler import (
    PostgresHandler,
)
from mindsdb.integrations.libs.response import RESPONSE_TYPE, HandlerResponse as Response
from mindsdb.integrations.libs.vectordatabase_handler import (
    FilterCondition,
    VectorStoreHandler,
    DistanceFunction,
    TableField,
    FilterOperator,
)
from mindsdb.integrations.libs.keyword_search_base import KeywordSearchBase
from mindsdb.integrations.utilities.sql_utils import KeywordSearchArgs
from mindsdb.utilities import log
from mindsdb.utilities.profiler import profiler
from mindsdb.utilities.context import context as ctx

logger = log.getLogger(__name__)


# todo Issue #7316 add support for different indexes and search algorithms e.g. cosine similarity or L2 norm
class PgVectorHandler(PostgresHandler, VectorStoreHandler, KeywordSearchBase):
    """This handler handles connection and execution of the PostgreSQL with pgvector extension statements."""

    name = "pgvector"

    def __init__(self, name: str, **kwargs):
        super().__init__(name=name, **kwargs)
        self._is_shared_db = False
        self._is_vector_registered = False
        # we get these from the connection args on PostgresHandler parent
        self._is_sparse = self.connection_args.get("is_sparse", False)
        self._vector_size = self.connection_args.get("vector_size", None)

        if self._is_sparse:
            if not self._vector_size:
                raise ValueError("vector_size is required when is_sparse=True")

                # Use inner product for sparse vectors
                distance_op = "<#>"

        else:
            distance_op = "<=>"
            if "distance" in self.connection_args:
                distance_ops = {
                    "l1": "<+>",
                    "l2": "<->",
                    "ip": "<#>",  # inner product
                    "cosine": "<=>",
                    "hamming": "<~>",
                    "jaccard": "<%>",
                }

                distance_op = distance_ops.get(self.connection_args["distance"])
                if distance_op is None:
                    raise ValueError(f"Wrong distance type. Allowed options are {list(distance_ops.keys())}")

        self.distance_op = distance_op
        self.connect()

    def get_metric_type(self) -> str:
        """
        Get the metric type from the distance ops

        """
        distance_ops_to_metric_type_map = {
            "<->": "vector_l2_ops",
            "<#>": "vector_ip_ops",
            "<=>": "vector_cosine_ops",
            "<+>": "vector_l1_ops",
            "<~>": "bit_hamming_ops",
            "<%>": "bit_jaccard_ops",
        }
        return distance_ops_to_metric_type_map.get(self.distance_op, "vector_cosine_ops")

    def _make_connection_args(self):
        cloud_pgvector_url = os.environ.get("KB_PGVECTOR_URL")
        # if no connection args and shared pg vector defined - use it
        if len(self.connection_args) == 0 and cloud_pgvector_url is not None:
            result = urlparse(cloud_pgvector_url)
            self.connection_args = {
                "host": result.hostname,
                "port": result.port,
                "user": result.username,
                "password": result.password,
                "database": result.path[1:],
            }
            self._is_shared_db = True
        return super()._make_connection_args()

    def get_tables(self) -> Response:
        # Hide list of tables from all users
        if self._is_shared_db:
            return Response(RESPONSE_TYPE.OK)
        return super().get_tables()

    def native_query(self, query, params=None) -> Response:
        # Prevent execute native queries
        if self._is_shared_db:
            return Response(RESPONSE_TYPE.OK)
        return super().native_query(query, params=params)

    def raw_query(self, query, params=None) -> Response:
        resp = super().native_query(query, params)
        if resp.resp_type == RESPONSE_TYPE.ERROR:
            raise RuntimeError(resp.error_message)
        if resp.resp_type == RESPONSE_TYPE.TABLE:
            return resp.data_frame

    @profiler.profile()
    def connect(self) -> psycopg.connection:
        """
        Handles the connection to a PostgreSQL database instance.
        """
        self.connection = super().connect()
        if self._is_vector_registered:
            return self.connection

        with self.connection.cursor() as cur:
            try:
                # load pg_vector extension
                cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
                logger.info("pg_vector extension loaded")

            except psycopg.Error as e:
                self.connection.rollback()
                logger.error(f"Error loading pg_vector extension, ensure you have installed it before running, {e}!")
                raise

        # register vector type with psycopg2 connection
        register_vector(self.connection)
        self._is_vector_registered = True

        return self.connection

    def add_full_text_index(self, table_name: str, column_name: str) -> Response:
        """
        Add a full text index to the specified column of the table.
        Args:
            table_name (str): Name of the table to add the index to.
            column_name (str): Name of the column to add the index to.
        Returns:
            Response: Response object indicating success or failure.
        """
        table_name = self._check_table(table_name)
        query = f"CREATE INDEX IF NOT EXISTS {table_name}_{column_name}_fts_idx ON {table_name} USING gin(to_tsvector('english', {column_name}))"
        self.raw_query(query)
        return Response(RESPONSE_TYPE.OK)

    @staticmethod
    def _translate_conditions(conditions: List[FilterCondition]) -> Tuple[List[dict], dict]:
        """
        Translate filter conditions to a dictionary
        """

        if conditions is None:
            conditions = []

        filter_conditions = []
        embedding_condition = None

        for condition in conditions:
            is_embedding = condition.column == "embeddings"

            parts = condition.column.split(".")
            key = Identifier(parts[0])

            # converts 'col.el1.el2' to col->'el1'->>'el2'
            if len(parts) > 1:
                # intermediate elements
                for el in parts[1:-1]:
                    key = BinaryOperation(op="->", args=[key, Constant(el)])

                # last element
                key = BinaryOperation(op="->>", args=[key, Constant(parts[-1])])

            type_cast = None
            value = condition.value
            if (
                isinstance(value, list)
                and len(value) > 0
                and condition.op in (FilterOperator.IN, FilterOperator.NOT_IN)
            ):
                value = condition.value[0]

            if isinstance(value, int):
                type_cast = "int"
            elif isinstance(value, float):
                type_cast = "float"
            if type_cast is not None:
                key = TypeCast(type_cast, key)

            item = {
                "name": key,
                "op": condition.op.value,
                "value": condition.value,
            }
            if is_embedding:
                embedding_condition = item
            else:
                filter_conditions.append(item)

        return filter_conditions, embedding_condition

    @staticmethod
    def _construct_where_clause(filter_conditions=None):
        """
        Construct where clauses from filter conditions
        """

        where_clause = None

        for item in filter_conditions:
            key = item["name"]

            if item["op"].lower() in ("in", "not in"):
                values = [Constant(i) for i in item["value"]]
                value = AstTuple(values)
            else:
                value = Constant(item["value"])
            condition = BinaryOperation(op=item["op"], args=[key, value])

            if where_clause is None:
                where_clause = condition
            else:
                where_clause = BinaryOperation(op="AND", args=[where_clause, condition])
        return where_clause

    @staticmethod
    def _construct_full_after_from_clause(
        where_clause: str,
        offset_clause: str,
        limit_clause: str,
    ) -> str:
        return f"{where_clause} {offset_clause} {limit_clause}"

    def _build_keyword_bm25_query(
        self,
        table_name: str,
        keyword_search_args: KeywordSearchArgs,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        offset: int = None,
    ):
        if columns is None:
            columns = ["id", "content", "metadata"]

        filter_conditions, _ = self._translate_conditions(conditions)
        where_clause = self._construct_where_clause(filter_conditions)

        if keyword_search_args:
            keyword_query_condition = BinaryOperation(
                op="@@",
                args=[
                    Function("to_tsvector", args=[Constant("english"), Identifier(keyword_search_args.column)]),
                    Function("websearch_to_tsquery", args=[Constant("english"), Constant(keyword_search_args.query)]),
                ],
            )

            if where_clause:
                where_clause = BinaryOperation(op="AND", args=[where_clause, keyword_query_condition])
            else:
                where_clause = keyword_query_condition

        distance = Function(
            "ts_rank_cd",
            args=[
                Function("to_tsvector", args=[Constant("english"), Identifier(keyword_search_args.column)]),
                Function("websearch_to_tsquery", args=[Constant("english"), Constant(keyword_search_args.query)]),
            ],
            alias=Identifier("distance"),
        )

        targets = [Identifier(col) for col in columns]
        targets.append(distance)

        limit_clause = Constant(limit) if limit else None
        offset_clause = Constant(offset) if offset else None

        return Select(
            targets=targets,
            from_table=Identifier(table_name),
            where=where_clause,
            limit=limit_clause,
            offset=offset_clause,
        )

    def _build_select_query(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        offset: int = None,
    ) -> Select:
        """
        given inputs, build string query
        """
        limit_clause = Constant(limit) if limit else None
        offset_clause = Constant(offset) if offset else None

        # translate filter conditions to dictionary
        filter_conditions, embedding_search = self._translate_conditions(conditions)

        # given filter conditions, construct where clause
        where_clause = self._construct_where_clause(filter_conditions)

        # Handle distance column specially since it's calculated, not stored
        modified_columns = []
        has_distance = False
        if columns is not None:
            for col in columns:
                if col == TableField.DISTANCE.value:
                    has_distance = True
                else:
                    modified_columns.append(col)
        else:
            modified_columns = ["id", "content", "embeddings", "metadata"]
            has_distance = True

        targets = [Identifier(col) for col in modified_columns]

        query = Select(
            targets=targets,
            from_table=Identifier(table_name),
            where=where_clause,
            limit=limit_clause,
            offset=offset_clause,
        )

        if embedding_search:
            search_vector = embedding_search["value"]

            if self._is_sparse:
                # Convert dict to sparse vector if needed
                if isinstance(search_vector, dict):
                    from pgvector.utils import SparseVector

                    embedding = SparseVector(search_vector, self._vector_size)
                    search_vector = embedding.to_text()
            else:
                # Convert list to vector string if needed
                if isinstance(search_vector, list):
                    search_vector = f"[{','.join(str(x) for x in search_vector)}]"

            vector_op = BinaryOperation(
                op=self.distance_op,
                args=[Identifier("embeddings"), Constant(search_vector)],
                alias=Identifier("distance"),
            )
            # Calculate distance as part of the query if needed
            if has_distance:
                query.targets.append(vector_op)

            query.order_by = [OrderBy(vector_op, direction="ASC")]

        return query

    def _check_table(self, table_name: str):
        # Apply namespace for a user
        if self._is_shared_db:
            company_id = ctx.company_id or "x"
            return f"t_{company_id}_{table_name}"
        return table_name

    def select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
    ) -> pd.DataFrame:
        """
        Retrieve the data from the SQL statement with eliminated rows that dont satisfy the WHERE condition
        """
        table_name = self._check_table(table_name)

        if columns is None:
            columns = ["id", "content", "embeddings", "metadata"]

        query = self._build_select_query(table_name, columns, conditions, limit, offset)
        query_str = self.renderer.get_string(query, with_failback=True)
        result = self.raw_query(query_str)

        # ensure embeddings are returned as string so they can be parsed by mindsdb
        if "embeddings" in columns:
            result["embeddings"] = result["embeddings"].astype(str)

        return result

    def keyword_select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
        keyword_search_args: KeywordSearchArgs = None,
    ) -> pd.DataFrame:
        table_name = self._check_table(table_name)

        if columns is None:
            columns = ["id", "content", "embeddings", "metadata"]

        query = self._build_keyword_bm25_query(table_name, keyword_search_args, columns, conditions, limit, offset)
        query_str = self.renderer.get_string(query, with_failback=True)
        result = self.raw_query(query_str)

        # ensure embeddings are returned as string so they can be parsed by mindsdb
        if "embeddings" in columns:
            result["embeddings"] = result["embeddings"].astype(str)

        return result

    def hybrid_search(
        self,
        table_name: str,
        embeddings: List[float],
        query: str = None,
        metadata: Dict[str, str] = None,
        distance_function=DistanceFunction.COSINE_DISTANCE,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Executes a hybrid search, combining semantic search and one or both of keyword/metadata search.

        For insight on the query construction, see: https://docs.pgvecto.rs/use-case/hybrid-search.html#advanced-search-merge-the-results-of-full-text-search-and-vector-search.

        Args:
            table_name(str): Name of underlying table containing content, embeddings, & metadata
            embeddings(List[float]): Embedding vector to perform semantic search against
            query(str): User query to convert into keywords for keyword search
            metadata(Dict[str, str]): Metadata filters to filter content rows against
            distance_function(DistanceFunction): Distance function used to compare embeddings vectors for semantic search

        Kwargs:
            id_column_name(str): Name of ID column in underlying table
            content_column_name(str): Name of column containing document content in underlying table
            embeddings_column_name(str): Name of column containing embeddings vectors in underlying table
            metadata_column_name(str): Name of column containing metadata key-value pairs in underlying table

        Returns:
            df(pd.DataFrame): Hybrid search result, sorted by hybrid search rank
        """
        if query is None and metadata is None:
            raise ValueError(
                "Must provide at least one of: query for keyword search, or metadata filters. For only embeddings search, use normal search instead."
            )

        id_column_name = kwargs.get("id_column_name", "id")
        content_column_name = kwargs.get("content_column_name", "content")
        embeddings_column_name = kwargs.get("embeddings_column_name", "embeddings")
        metadata_column_name = kwargs.get("metadata_column_name", "metadata")
        # Filter by given metadata for semantic search & full text search CTEs, if present.
        where_clause = " WHERE "
        if metadata is None:
            where_clause = ""
            metadata = {}
        for i, (k, v) in enumerate(metadata.items()):
            where_clause += f"{metadata_column_name}->>'{k}' = '{v}'"
            if i < len(metadata.items()) - 1:
                where_clause += " AND "

        # See https://docs.pgvecto.rs/use-case/hybrid-search.html#advanced-search-merge-the-results-of-full-text-search-and-vector-search.
        #
        # We can break down the below query as follows:
        #
        # Start with a CTE (Common Table Expression) called semantic_search (https://www.postgresql.org/docs/current/queries-with.html).
        # This expression calculates rank by the defined distance function, which measures the distance between the
        # embeddings column and the given embeddings vector. Results are ordered by this rank.
        #
        # Next, define another CTE called full_text_search if we are doing keyword search.
        # This calculates rank using the built-in ts_rank function (https://www.postgresql.org/docs/current/textsearch-controls.html#TEXTSEARCH-RANKING).
        # We convert the content column to a ts_vector and match rows for the given tsquery in the content column. Results are ordered by this ts_rank.
        #
        # For both of these CTEs, we filter by any given metadata fields.
        #
        # See https://www.postgresql.org/docs/current/textsearch-controls.html#TEXTSEARCH-PARSING-DOCUMENTS for to_tsvector
        # See https://www.postgresql.org/docs/current/functions-textsearch.html#FUNCTIONS-TEXTSEARCH for tsquery syntax
        #
        # Finally, we use a FULL OUTER JOIN to SELECT from both CTEs defined above.
        # The COALESCE function is used to handle cases where one CTE has null values.
        #
        # Or, if we are only doing metadata search, we leave out the JOIN & full text search CTEs.
        #
        # We calculate the final "hybrid" rank by summing the reciprocals of the ranks from each individual CTE.
        semantic_search_cte = f"""WITH semantic_search AS (
    SELECT {id_column_name}, {content_column_name}, {embeddings_column_name},
    RANK () OVER (ORDER BY {embeddings_column_name} {distance_function.value} '{str(embeddings)}') AS rank
    FROM {table_name}{where_clause}
    ORDER BY {embeddings_column_name} {distance_function.value} '{str(embeddings)}'::vector
    )"""

        full_text_search_cte = ""
        if query is not None:
            ts_vector_clause = (
                f"WHERE to_tsvector('english', {content_column_name}) @@ plainto_tsquery('english', '{query}')"
            )
            if metadata:
                ts_vector_clause = (
                    f"AND to_tsvector('english', {content_column_name}) @@ plainto_tsquery('english', '{query}')"
                )
            full_text_search_cte = f""",
    full_text_search AS (
    SELECT {id_column_name}, {content_column_name}, {embeddings_column_name},
    RANK () OVER (ORDER BY ts_rank(to_tsvector('english', {content_column_name}), plainto_tsquery('english', '{query}')) DESC) AS rank
    FROM {table_name}{where_clause}
    {ts_vector_clause}
    ORDER BY ts_rank(to_tsvector('english', {content_column_name}), plainto_tsquery('english', '{query}')) DESC
    )"""

        hybrid_select = """
    SELECT * FROM semantic_search"""
        if query is not None:
            hybrid_select = f"""
    SELECT
        COALESCE(semantic_search.{id_column_name}, full_text_search.{id_column_name}) AS id,
        COALESCE(semantic_search.{content_column_name}, full_text_search.{content_column_name}) AS content,
        COALESCE(semantic_search.{embeddings_column_name}, full_text_search.{embeddings_column_name}) AS embeddings,
        COALESCE(1.0 / (1 + semantic_search.rank), 0.0) + COALESCE(1.0 / (1 + full_text_search.rank), 0.0) AS rank
    FROM semantic_search FULL OUTER JOIN full_text_search USING ({id_column_name}) ORDER BY rank DESC;
        """

        full_search_query = f"{semantic_search_cte}{full_text_search_cte}{hybrid_select}"
        return self.raw_query(full_search_query)

    def create_table(self, table_name: str):
        """Create a table with a vector column."""
        with self.connection.cursor() as cur:
            # For sparse vectors, use sparsevec type
            vector_column_type = "sparsevec" if self._is_sparse else "vector"

            # Vector size is required for sparse vectors, optional for dense
            if self._is_sparse and not self._vector_size:
                raise ValueError("vector_size is required for sparse vectors")

            # Add vector size specification only if provided
            size_spec = f"({self._vector_size})" if self._vector_size is not None else "()"
            if vector_column_type == "vector":
                size_spec = ""

            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id TEXT PRIMARY KEY,
                    embeddings {vector_column_type}{size_spec},
                    content TEXT,
                    metadata JSONB
                )
            """)
            self.connection.commit()

    def insert(self, table_name: str, data: pd.DataFrame):
        """
        Insert data into the pgvector table database.
        """
        table_name = self._check_table(table_name)

        if "metadata" in data.columns:
            data["metadata"] = data["metadata"].apply(json.dumps)

        resp = super().insert(table_name, data)
        if resp.resp_type == RESPONSE_TYPE.ERROR:
            raise RuntimeError(resp.error_message)
        if resp.resp_type == RESPONSE_TYPE.TABLE:
            return resp.data_frame

    def update(self, table_name: str, data: pd.DataFrame, key_columns: List[str] = None):
        """
        Udate data into the pgvector table database.
        """
        table_name = self._check_table(table_name)

        where = None
        update_columns = {}

        for col in data.columns:
            value = Parameter("%s")

            if col in key_columns:
                cond = BinaryOperation(op="=", args=[Identifier(col), value])
                if where is None:
                    where = cond
                else:
                    where = BinaryOperation(op="AND", args=[where, cond])
            else:
                update_columns[col] = value

        query = Update(table=Identifier(table_name), update_columns=update_columns, where=where)

        if TableField.METADATA.value in data.columns:

            def fnc(v):
                if isinstance(v, dict):
                    return json.dumps(v)

            data[TableField.METADATA.value] = data[TableField.METADATA.value].apply(fnc)

            data = data.astype({TableField.METADATA.value: str})

        transposed_data = []
        for _, record in data.iterrows():
            row = [record[col] for col in update_columns.keys()]
            for key_column in key_columns:
                row.append(record[key_column])
            transposed_data.append(row)

        query_str = self.renderer.get_string(query)
        self.raw_query(query_str, transposed_data)

    def delete(self, table_name: str, conditions: List[FilterCondition] = None):
        table_name = self._check_table(table_name)

        filter_conditions, _ = self._translate_conditions(conditions)
        where_clause = self._construct_where_clause(filter_conditions)

        query = Delete(table=Identifier(table_name), where=where_clause)
        query_str = self.renderer.get_string(query, with_failback=True)
        self.raw_query(query_str)

    def drop_table(self, table_name: str, if_exists=True):
        """
        Run a drop table query on the pgvector database.
        """
        table_name = self._check_table(table_name)
        self.raw_query(f"DROP TABLE IF EXISTS {table_name}")

    def create_index(
        self,
        table_name: str,
        column_name: str = "embeddings",
        index_type: Literal["ivfflat", "hnsw"] = "hnsw",
        metric_type: str = None,
    ):
        """
        Create an index on the pgvector table.
        Args:
            table_name (str): Name of the table to create the index on.
            column_name (str): Name of the column to create the index on.
            index_type (str): Type of the index to create. Supported types are 'ivfflat' and 'hnsw'.
            metric_type (str): Metric type for the index. Supported types are 'vector_l2_ops', 'vector_ip_ops', and 'vector_cosine_ops'.
        """
        if metric_type is None:
            metric_type = self.get_metric_type()
        # Check if the index type is supported
        if index_type not in ["ivfflat", "hnsw"]:
            raise ValueError("Invalid index type. Supported types are 'ivfflat' and 'hnsw'.")
        table_name = self._check_table(table_name)
        # first we make sure embedding dimension is set
        embedding_dim_size_df = self.raw_query(f"SELECT vector_dims({column_name}) FROM {table_name} LIMIT 1")
        # check if answer is empty
        if embedding_dim_size_df.empty:
            raise ValueError("Could not determine embedding dimension size. Make sure that knowledge base isn't empty")
        try:
            embedding_dim = int(embedding_dim_size_df.iloc[0, 0])
            # alter table to add dimension
            self.raw_query(f"ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE vector({embedding_dim})")
        except Exception:
            raise ValueError("Could not determine embedding dimension size. Make sure that knowledge base isn't empty")

        # Create the index
        self.raw_query(f"CREATE INDEX ON {table_name} USING {index_type} ({column_name} {metric_type})")
