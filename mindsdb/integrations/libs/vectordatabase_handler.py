import ast
import uuid
from enum import Enum
from typing import Any, List, Optional

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
from mindsdb.utilities.log import get_log

from ..utilities.sql_utils import query_traversal
from .base import BaseHandler

LOG = get_log("VectorStoreHandler")


class FilterOperator(Enum):
    """
    Enum for filter operators.
    """

    EQUAL = "="
    NOT_EQUAL = "!="
    LESS_THAN = "<"
    LESS_THAN_OR_EQUAL = "<="
    GREATER_THAN = ">"
    GREATER_THAN_OR_EQUAL = ">="
    IN = "IN"
    NOT_IN = "NOT IN"
    BETWEEN = "BETWEEN"
    NOT_BETWEEN = "NOT BETWEEN"
    LIKE = "LIKE"
    NOT_LIKE = "NOT LIKE"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"


class FilterCondition:
    """
    Base class for filter conditions.
    """

    def __init__(self, column: str, op: FilterOperator, value: Any):
        self.column = column
        self.op = op
        self.value = value

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, FilterCondition):
            return (
                self.column == __value.column
                and self.op == __value.op
                and self.value == __value.value
            )
        else:
            return False

    def __repr__(self) -> str:
        return f"""
            FilterCondition(
                column={self.column},
                op={self.op},
                value={self.value}
            )
        """


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

    def __init__(self, name: str):
        super().__init__(name)

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
                        right_hand = [item.value for item in node.args[1].items]
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

    def _dispatch_create_table(self, query: CreateTable) -> HandlerResponse:
        """
        Dispatch create table query to the appropriate method.
        """
        # parse key arguments
        table_name = query.name.parts[-1]
        if_not_exists = getattr(query, "if_not_exists", False)
        return self.create_table(table_name, if_not_exists=if_not_exists)

    def _dispatch_drop_table(self, query: DropTables) -> HandlerResponse:
        """
        Dispatch drop table query to the appropriate method.
        """
        # parse key arguments
        table_names = [table.parts[-1] for table in query.tables]
        if_exists = getattr(query, "if_exists", False)
        for table_name in table_names:
            self.drop_table(table_name, if_exists=if_exists)
        return HandlerResponse(resp_type=RESPONSE_TYPE.OK)

    def _dispatch_insert(self, query: Insert) -> HandlerResponse:
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

        # get id column if it is present
        if "id" in columns:
            id_col_index = columns.index("id")
            ids = [self._value_or_self(row[id_col_index]) for row in query.values]
        else:
            ids = [uuid.uuid4().hex for _ in query.values]

        # get content column if it is present
        if TableField.CONTENT.value in columns:
            content_col_index = columns.index("content")
            content = [
                self._value_or_self(row[content_col_index]) for row in query.values
            ]
        else:
            content = None

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
        data = pd.DataFrame(
            {
                TableField.ID.value: ids,
                TableField.CONTENT.value: content,
                TableField.EMBEDDINGS.value: embeddings,
                TableField.METADATA.value: metadata,
            }
        )

        # dispatch insert
        return self.insert(table_name, data, columns=columns)

    def _dispatch_update(self, query: Update) -> HandlerResponse:
        """
        Dispatch update query to the appropriate method.
        """
        raise NotImplementedError("Update query is not supported!")

    def _dispatch_delete(self, query: Delete) -> HandlerResponse:
        """
        Dispatch delete query to the appropriate method.
        """
        # parse key arguments
        table_name = query.table.parts[-1]
        where_statement = query.where
        conditions = self._extract_conditions(where_statement)

        # dispatch delete
        return self.delete(table_name, conditions=conditions)

    def _dispatch_select(self, query: Select) -> HandlerResponse:
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
            return dispatch_router[type(query)](query)
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
        self, table_name: str, data: pd.DataFrame, columns: List[str] = None
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
        self, table_name: str, data: pd.DataFrame, columns: List[str] = None
    ) -> HandlerResponse:
        """Update data in table

        Args:
            table_name (str): table name
            data (pd.DataFrame): data to update
            columns (List[str]): columns to update

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
    ) -> HandlerResponse:
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
            data_frame=data,
        )
