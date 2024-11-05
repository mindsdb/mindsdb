import os
import json
from enum import Enum
from typing import Dict, List, Union
from urllib.parse import urlparse

import pandas as pd
import psycopg
from mindsdb_sql.parser.ast import Parameter, Identifier, Update, BinaryOperation
from pgvector.psycopg import register_vector

from mindsdb.integrations.handlers.postgres_handler.postgres_handler import (
    PostgresHandler,
)
from mindsdb.integrations.libs.response import RESPONSE_TYPE, HandlerResponse as Response
from mindsdb.integrations.libs.vectordatabase_handler import (
    FilterCondition,
    VectorStoreHandler,
    DistanceFunction
)
from mindsdb.utilities import log
from mindsdb.utilities.profiler import profiler
from mindsdb.utilities.context import context as ctx

logger = log.getLogger(__name__)


# todo Issue #7316 add support for different indexes and search algorithms e.g. cosine similarity or L2 norm
class PgVectorHandler(VectorStoreHandler, PostgresHandler):
    """This handler handles connection and execution of the PostgreSQL with pgvector extension statements."""

    name = "pgvector"

    def __init__(self, name: str, **kwargs):

        super().__init__(name=name, **kwargs)
        self._is_shared_db = False
        self.connect()

    def _make_connection_args(self):
        cloud_pgvector_url = os.environ.get('KB_PGVECTOR_URL')
        if cloud_pgvector_url is not None:
            result = urlparse(cloud_pgvector_url)
            self.connection_args = {
                'host': result.hostname,
                'port': result.port,
                'user': result.username,
                'password': result.password,
                'database': result.path[1:]
            }
            self._is_shared_db = True
        return super()._make_connection_args()

    def get_tables(self) -> Response:
        # Hide list of tables from all users
        if self._is_shared_db:
            return Response(RESPONSE_TYPE.OK)
        return super().get_tables()

    def native_query(self, query) -> Response:
        # Prevent execute native queries
        if self._is_shared_db:
            return Response(RESPONSE_TYPE.OK)
        return super().native_query(query)

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

        with self.connection.cursor() as cur:
            try:
                # load pg_vector extension
                cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
                logger.info("pg_vector extension loaded")

            except psycopg.Error as e:
                self.connection.rollback()
                logger.error(
                    f"Error loading pg_vector extension, ensure you have installed it before running, {e}!"
                )
                raise

        # register vector type with psycopg2 connection
        register_vector(self.connection)

        return self.connection

    @staticmethod
    def _translate_conditions(conditions: List[FilterCondition]) -> Union[dict, None]:
        """
        Translate filter conditions to a dictionary
        """

        if conditions is None:
            return {}

        return {
            condition.column.split(".")[-1]: {
                "op": condition.op.value,
                "value": condition.value,
            }
            for condition in conditions
        }

    @staticmethod
    def _construct_where_clause(filter_conditions=None):
        """
        Construct where clauses from filter conditions
        """
        if filter_conditions is None:
            return ""

        where_clauses = []

        for key, value in filter_conditions.items():
            if key == "embeddings":
                continue
            if value['op'].lower() == 'in':
                values = list(repr(i) for i in value['value'])
                value['value'] = '({})'.format(', '.join(values))
            else:
                value['value'] = repr(value['value'])
            where_clauses.append(f'{key} {value["op"]} {value["value"]}')

        if len(where_clauses) > 1:
            return f"WHERE{' AND '.join(where_clauses)}"
        elif len(where_clauses) == 1:
            return f"WHERE {where_clauses[0]}"
        else:
            return ""

    @staticmethod
    def _construct_full_after_from_clause(
        offset_clause: str,
        limit_clause: str,
        where_clause: str,
    ) -> str:

        return f"{where_clause} {offset_clause} {limit_clause}"

    def _build_select_query(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        offset: int = None,
    ) -> str:
        """
        given inputs, build string query
        """
        limit_clause = f"LIMIT {limit}" if limit else ""
        offset_clause = f"OFFSET {offset}" if offset else ""

        # translate filter conditions to dictionary
        filter_conditions = self._translate_conditions(conditions)

        # check if search vector is in filter conditions
        embedding_search = filter_conditions.get("embeddings", None)

        # given filter conditions, construct where clause
        where_clause = self._construct_where_clause(filter_conditions)

        # construct full after from clause, where clause + offset clause + limit clause
        after_from_clause = self._construct_full_after_from_clause(
            where_clause, offset_clause, limit_clause
        )

        if columns is None:
            targets = '*'
        else:
            targets = ', '.join(columns)


        if filter_conditions:

            if embedding_search:
                # if search vector, return similar rows, apply other filters after if any
                search_vector = filter_conditions["embeddings"]["value"][0]
                filter_conditions.pop("embeddings")
                return f"SELECT {targets} FROM {table_name} ORDER BY embeddings <=> '{search_vector}' {after_from_clause}"
            else:
                # if filter conditions, return filtered rows
                return f"SELECT {targets} FROM {table_name} {after_from_clause}"
        else:
            # if no filter conditions, return all rows
            return f"SELECT {targets} FROM {table_name} {after_from_clause}"

    def _check_table(self, table_name: str):
        # Apply namespace for a user
        if self._is_shared_db:
            company_id = ctx.company_id or 'x'
            return f't_{company_id}_{table_name}'
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
        result = self.raw_query(query)

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
        distance_function = DistanceFunction.COSINE_DISTANCE,
        **kwargs
    ) -> pd.DataFrame:
        '''
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
        '''
        if query is None and metadata is None:
            raise ValueError('Must provide at least one of: query for keyword search, or metadata filters. For only embeddings search, use normal search instead.')

        id_column_name = kwargs.get('id_column_name', 'id')
        content_column_name = kwargs.get('content_column_name', 'content')
        embeddings_column_name = kwargs.get('embeddings_column_name', 'embeddings')
        metadata_column_name = kwargs.get('metadata_column_name', 'metadata')
        # Filter by given metadata for semantic search & full text search CTEs, if present.
        where_clause = ' WHERE '
        if metadata is None:
            where_clause = ''
            metadata = {}
        for i, (k, v) in enumerate(metadata.items()):
            where_clause += f"{metadata_column_name}->>'{k}' = '{v}'"
            if i < len(metadata.items()) - 1:
                where_clause += ' AND '

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
        semantic_search_cte = f'''WITH semantic_search AS (
    SELECT {id_column_name}, {content_column_name}, {embeddings_column_name},
    RANK () OVER (ORDER BY {embeddings_column_name} {distance_function.value} '{str(embeddings)}') AS rank
    FROM {table_name}{where_clause}
    ORDER BY {embeddings_column_name} {distance_function.value} '{str(embeddings)}'::vector
    )'''

        full_text_search_cte = ''
        if query is not None:
            ts_vector_clause = f"WHERE to_tsvector('english', {content_column_name}) @@ plainto_tsquery('english', '{query}')"
            if metadata:
                ts_vector_clause = f"AND to_tsvector('english', {content_column_name}) @@ plainto_tsquery('english', '{query}')"
            full_text_search_cte = f''',
    full_text_search AS (
    SELECT {id_column_name}, {content_column_name}, {embeddings_column_name},
    RANK () OVER (ORDER BY ts_rank(to_tsvector('english', {content_column_name}), plainto_tsquery('english', '{query}')) DESC) AS rank
    FROM {table_name}{where_clause}
    {ts_vector_clause}
    ORDER BY ts_rank(to_tsvector('english', {content_column_name}), plainto_tsquery('english', '{query}')) DESC
    )'''

        hybrid_select = '''
    SELECT * FROM semantic_search'''
        if query is not None:
            hybrid_select = f'''
    SELECT
        COALESCE(semantic_search.{id_column_name}, full_text_search.{id_column_name}) AS id,
        COALESCE(semantic_search.{content_column_name}, full_text_search.{content_column_name}) AS content,
        COALESCE(semantic_search.{embeddings_column_name}, full_text_search.{embeddings_column_name}) AS embeddings,
        COALESCE(1.0 / (1 + semantic_search.rank), 0.0) + COALESCE(1.0 / (1 + full_text_search.rank), 0.0) AS rank
    FROM semantic_search FULL OUTER JOIN full_text_search USING ({id_column_name}) ORDER BY rank DESC;
        '''

        full_search_query = f'{semantic_search_cte}{full_text_search_cte}{hybrid_select}'
        return self.raw_query(full_search_query)

    def create_table(self, table_name: str, if_not_exists=True):
        """
        Run a create table query on the pgvector database.
        """
        table_name = self._check_table(table_name)

        query = f"CREATE TABLE IF NOT EXISTS {table_name} (id text PRIMARY KEY, content text, embeddings vector, metadata jsonb)"
        self.raw_query(query)

    def insert(
        self, table_name: str, data: pd.DataFrame
    ):
        """
        Insert data into the pgvector table database.
        """
        table_name = self._check_table(table_name)

        data_dict = data.to_dict(orient="list")

        if 'metadata' in data_dict:
            data_dict['metadata'] = [json.dumps(i) for i in data_dict['metadata']]
        transposed_data = list(zip(*data_dict.values()))

        columns = ", ".join(data.keys())
        values = ", ".join(["%s"] * len(data.keys()))

        insert_statement = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

        self.raw_query(insert_statement, params=transposed_data)

    def update(
        self, table_name: str, data: pd.DataFrame, key_columns: List[str] = None
    ):
        """
        Udate data into the pgvector table database.
        """
        table_name = self._check_table(table_name)

        where = None
        update_columns = {}

        for col in data.columns:
            value = Parameter('%s')

            if col in key_columns:
                cond = BinaryOperation(
                    op='=',
                    args=[Identifier(col), value]
                )
                if where is None:
                    where = cond
                else:
                    where = BinaryOperation(
                        op='AND',
                        args=[where, cond]
                    )
            else:
                update_columns[col] = value

        query = Update(
            table=Identifier(table_name),
            update_columns=update_columns,
            where=where
        )

        transposed_data = []
        for _, record in data.iterrows():
            row = [
                record[col]
                for col in update_columns.keys()
            ]
            for key_column in key_columns:
                row.append(record[key_column])
            transposed_data.append(row)

        query_str = self.renderer.get_string(query)
        self.raw_query(query_str, transposed_data)

    def delete(
        self, table_name: str, conditions: List[FilterCondition] = None
    ):
        table_name = self._check_table(table_name)

        filter_conditions = self._translate_conditions(conditions)
        where_clause = self._construct_where_clause(filter_conditions)

        query = (
            f"DELETE FROM {table_name} {where_clause}"
        )
        self.raw_query(query)

    def drop_table(self, table_name: str, if_exists=True):
        """
        Run a drop table query on the pgvector database.
        """
        table_name = self._check_table(table_name)
        self.raw_query(f"DROP TABLE IF EXISTS {table_name}")

