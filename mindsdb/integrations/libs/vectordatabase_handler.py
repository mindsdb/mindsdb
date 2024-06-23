import ast
import hashlib
from enum import Enum
from typing import List, Optional

import pandas as pd
from mindsdb_sql.parser.ast import (
    BinaryOperation,
    Constant,
    CreateTable,
    Delete,
    DropTables,
    Insert,
    Select,
    Star,
    Tuple,
    Update,
)
from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.integrations.libs.response import RESPONSE_TYPE, HandlerResponse
from mindsdb.utilities import log
from mindsdb.integrations.utilities.sql_utils import conditions_to_filter, FilterCondition, FilterOperator

from ..utilities.sql_utils import query_traversal
from .base import BaseHandler

LOG = log.getLogger(__name__)


class TableField(Enum):
    """
    Enum for table fields.
    """

    ID = "id"
    CONTENT = "content"
    EMBEDDINGS = "embeddings"
    METADATA = "metadata"
    SEARCH_VECTOR = "search_vector"
    DISTANCE = "distance"


class VectorStoreHandler(BaseHandler):
    """
    Base class for handlers associated to vector databases.
    """

    SCHEMA = [
        {
            "name": TableField.ID.value,
            "data_type": "string",
        },
        {
            "name": TableField.CONTENT.value,
            "data_type": "string",
        },
        {
            "name": TableField.EMBEDDINGS.value,
            "data_type": "list",
        },
        {
            "name": TableField.METADATA.value,
            "data_type": "json",
        },
    ]

    def validate_connection_parameters(self, name, **kwargs):
        """Create validation for input parameters."""

        return NotImplementedError()

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def disconnect(self):
        pass

    def _value_or_self(self, value):
        if isinstance(value, Constant):
            return value.value
        else:
            return value

    def _extract_conditions(self, where_statement) -> Optional[List[FilterCondition]]:
        conditions = []
        # parse conditions
        if where_statement is not None:
            # dfs to get all binary operators in the where statement
            def _extract_comparison_conditions(node, **kwargs):
                if isinstance(node, BinaryOperation):
                    # if the op is and, continue
                    # TODO: need to handle the OR case
                    if node.op.upper() == "AND":
                        return
                    op = FilterOperator(node.op.upper())
                    # unquote the left hand side
                    left_hand = node.args[0].parts[-1].strip("`")
                    if isinstance(node.args[1], Constant):
                        if left_hand == TableField.SEARCH_VECTOR.value:
                            right_hand = ast.literal_eval(node.args[1].value)
                        else:
                            right_hand = node.args[1].value
                    elif isinstance(node.args[1], Tuple):
                        # Constant could be actually a list i.e. [1.2, 3.2]
                        right_hand = [
                            ast.literal_eval(item.value)
                            if isinstance(item, Constant)
                            and not isinstance(item.value, list)
                            else item.value
                            for item in node.args[1].items
                        ]
                    else:
                        raise Exception(f"Unsupported right hand side: {node.args[1]}")
                    conditions.append(
                        FilterCondition(column=left_hand, op=op, value=right_hand)
                    )

            query_traversal(where_statement, _extract_comparison_conditions)

            # try to treat conditions that are not in TableField as metadata conditions
            for condition in conditions:
                if not self._is_condition_allowed(condition):
                    condition.column = (
                        TableField.METADATA.value + "." + condition.column
                    )

        else:
            conditions = None

        return conditions

    def _is_columns_allowed(self, columns: List[str]) -> bool:
        """
        Check if columns are allowed.
        """
        allowed_columns = set([col["name"] for col in self.SCHEMA])
        return set(columns).issubset(allowed_columns)

    def _is_condition_allowed(self, condition: FilterCondition) -> bool:
        allowed_field_values = set([field.value for field in TableField])
        if condition.column in allowed_field_values:
            return True
        else:
            # check if column is a metadata column
            if condition.column.startswith(TableField.METADATA.value):
                return True
            else:
                return False

    def _dispatch_create_table(self, query: CreateTable):
        """
        Dispatch create table query to the appropriate method.
        """
        # parse key arguments
        table_name = query.name.parts[-1]
        if_not_exists = getattr(query, "if_not_exists", False)
        return self.create_table(table_name, if_not_exists=if_not_exists)

    def _dispatch_drop_table(self, query: DropTables):
        """
        Dispatch drop table query to the appropriate method.
        """
        table_name = query.tables[0].parts[-1]
        if_exists = getattr(query, "if_exists", False)

        return self.drop_table(table_name, if_exists=if_exists)

    def _dispatch_insert(self, query: Insert):
        """
        Dispatch insert query to the appropriate method.
        """
        # parse key arguments
        table_name = query.table.parts[-1]
        columns = [column.name for column in query.columns]

        if not self._is_columns_allowed(columns):
            raise Exception(
                f"Columns {columns} not allowed."
                f"Allowed columns are {[col['name'] for col in self.SCHEMA]}"
            )

        # get content column if it is present
        if TableField.CONTENT.value in columns:
            content_col_index = columns.index("content")
            content = [
                self._value_or_self(row[content_col_index]) for row in query.values
            ]
        else:
            content = None

        # get id column if it is present
        ids = None
        if TableField.ID.value in columns:
            id_col_index = columns.index("id")
            ids = [self._value_or_self(row[id_col_index]) for row in query.values]
        elif TableField.CONTENT.value is None:
            raise Exception("Content or id is required!")

        # get embeddings column if it is present
        if TableField.EMBEDDINGS.value in columns:
            embeddings_col_index = columns.index("embeddings")
            embeddings = [
                ast.literal_eval(self._value_or_self(row[embeddings_col_index]))
                for row in query.values
            ]
        else:
            raise Exception("Embeddings column is required!")

        if TableField.METADATA.value in columns:
            metadata_col_index = columns.index("metadata")
            metadata = [
                ast.literal_eval(self._value_or_self(row[metadata_col_index]))
                for row in query.values
            ]
        else:
            metadata = None

        # create dataframe
        data = {
            TableField.CONTENT.value: content,
            TableField.EMBEDDINGS.value: embeddings,
            TableField.METADATA.value: metadata,
        }
        if ids is not None:
            data[TableField.ID.value] = ids

        return self.do_upsert(table_name, pd.DataFrame(data))

    def _dispatch_update(self, query: Update):
        """
        Dispatch update query to the appropriate method.
        """
        table_name = query.table.parts[-1]

        row = {}
        for k, v in query.update_columns.items():
            k = k.lower()
            if isinstance(v, Constant):
                v = v.value
            if k == TableField.EMBEDDINGS.value and isinstance(v, str):
                # it could be embeddings in string
                try:
                    v = eval(v)
                except Exception:
                    pass
            row[k] = v

        filters = conditions_to_filter(query.where)
        row.update(filters)

        # checks
        if TableField.EMBEDDINGS.value not in row:
            raise Exception("Embeddings column is required!")

        if TableField.CONTENT.value not in row:
            raise Exception("Content is required!")

        # store
        df = pd.DataFrame([row])

        return self.do_upsert(table_name, df)

    def do_upsert(self, table_name, df):
        # if handler supports it, call upsert method

        id_col = TableField.ID.value
        content_col = TableField.CONTENT.value

        def gen_hash(v):
            return hashlib.md5(str(v).encode()).hexdigest()

        if id_col not in df.columns:
            # generate for all
            df[id_col] = df[content_col].apply(gen_hash)
        else:
            # generate for empty
            for i in range(len(df)):
                if pd.isna(df.loc[i, id_col]):
                    df.loc[i, id_col] = gen_hash(df.loc[i, content_col])

        # remove duplicated ids
        df = df.drop_duplicates([TableField.ID.value])

        # id is string TODO is it ok?
        df[id_col] = df[id_col].apply(str)

        if hasattr(self, 'upsert'):
            self.upsert(table_name, df)
            return

        # find existing ids
        res = self.select(
            table_name,
            columns=[id_col],
            conditions=[
                FilterCondition(column=id_col, op=FilterOperator.IN, value=list(df[id_col]))
            ]
        )
        existed_ids = list(res[id_col])

        # update existed
        df_update = df[df[id_col].isin(existed_ids)]
        df_insert = df[~df[id_col].isin(existed_ids)]

        if not df_update.empty:
            self.update(table_name, df_update, [id_col])
        if not df_insert.empty:
            self.insert(table_name, df_insert)

    def _dispatch_delete(self, query: Delete):
        """
        Dispatch delete query to the appropriate method.
        """
        # parse key arguments
        table_name = query.table.parts[-1]
        where_statement = query.where
        conditions = self._extract_conditions(where_statement)

        # dispatch delete
        return self.delete(table_name, conditions=conditions)

    def _dispatch_select(self, query: Select):
        """
        Dispatch select query to the appropriate method.
        """
        # parse key arguments
        table_name = query.from_table.parts[-1]
        # if targets are star, select all columns
        if isinstance(query.targets[0], Star):
            columns = [col["name"] for col in self.SCHEMA]
        else:
            columns = [col.parts[-1] for col in query.targets]

        if not self._is_columns_allowed(columns):
            raise Exception(
                f"Columns {columns} not allowed."
                f"Allowed columns are {[col['name'] for col in self.SCHEMA]}"
            )

        # check if columns are allowed
        where_statement = query.where
        conditions = self._extract_conditions(where_statement)

        # get offset and limit
        offset = query.offset.value if query.offset is not None else None
        limit = query.limit.value if query.limit is not None else None

        # dispatch select
        return self.select(
            table_name,
            columns=columns,
            conditions=conditions,
            offset=offset,
            limit=limit,
        )

    def _dispatch(self, query: ASTNode) -> HandlerResponse:
        """
        Parse and Dispatch query to the appropriate method.
        """
        dispatch_router = {
            CreateTable: self._dispatch_create_table,
            DropTables: self._dispatch_drop_table,
            Insert: self._dispatch_insert,
            Update: self._dispatch_update,
            Delete: self._dispatch_delete,
            Select: self._dispatch_select,
        }
        if type(query) in dispatch_router:
            resp = dispatch_router[type(query)](query)
            if resp is not None:
                return HandlerResponse(
                    resp_type=RESPONSE_TYPE.TABLE,
                    data_frame=resp
                )
            else:
                return HandlerResponse(resp_type=RESPONSE_TYPE.OK)

        else:
            raise NotImplementedError(f"Query type {type(query)} not implemented.")

    def query(self, query: ASTNode) -> HandlerResponse:
        """
        Receive query as AST (abstract syntax tree) and act upon it somehow.

        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INSERT, DELETE, etc

        Returns:
            HandlerResponse
        """
        return self._dispatch(query)

    def create_table(self, table_name: str, if_not_exists=True) -> HandlerResponse:
        """Create table

        Args:
            table_name (str): table name
            if_not_exists (bool): if True, do nothing if table exists

        Returns:
            HandlerResponse
        """
        raise NotImplementedError()

    def drop_table(self, table_name: str, if_exists=True) -> HandlerResponse:
        """Drop table

        Args:
            table_name (str): table name
            if_exists (bool): if True, do nothing if table does not exist

        Returns:
            HandlerResponse
        """
        raise NotImplementedError()

    def insert(
        self, table_name: str, data: pd.DataFrame
    ) -> HandlerResponse:
        """Insert data into table

        Args:
            table_name (str): table name
            data (pd.DataFrame): data to insert
            columns (List[str]): columns to insert

        Returns:
            HandlerResponse
        """
        raise NotImplementedError()

    def update(
        self, table_name: str, data: pd.DataFrame, key_columns: List[str] = None
    ):
        """Update data in table

        Args:
            table_name (str): table name
            data (pd.DataFrame): data to update
            key_columns (List[str]): key to  to update

        Returns:
            HandlerResponse
        """
        raise NotImplementedError()

    def delete(
        self, table_name: str, conditions: List[FilterCondition] = None
    ) -> HandlerResponse:
        """Delete data from table

        Args:
            table_name (str): table name
            conditions (List[FilterCondition]): conditions to delete

        Returns:
            HandlerResponse
        """
        raise NotImplementedError()

    def select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
    ) -> pd.DataFrame:
        """Select data from table

        Args:
            table_name (str): table name
            columns (List[str]): columns to select
            conditions (List[FilterCondition]): conditions to select

        Returns:
            HandlerResponse
        """
        raise NotImplementedError()

    def get_columns(self, table_name: str) -> HandlerResponse:
        # return a fixed set of columns
        data = pd.DataFrame(self.SCHEMA)
        data.columns = ["COLUMN_NAME", "DATA_TYPE"]
        return HandlerResponse(
            resp_type=RESPONSE_TYPE.DATA,
            data_frame=data,
        )
