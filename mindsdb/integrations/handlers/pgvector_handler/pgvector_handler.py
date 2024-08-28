import os
import json
from typing import List, Union
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

