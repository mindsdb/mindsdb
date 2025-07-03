from typing import Any, List, Optional
import ast as py_ast

import pandas as pd
from mindsdb_sql_parser.ast import ASTNode, Select, Insert, Update, Delete, Star
from mindsdb_sql_parser.ast.select.identifier import Identifier

from mindsdb.integrations.utilities.sql_utils import (
    extract_comparison_conditions,
    filter_dataframe,
    sort_dataframe,
    FilterCondition,
    FilterOperator,
    SortColumn,
)
from mindsdb.integrations.libs.base import BaseHandler
from mindsdb.integrations.libs.api_handler_exceptions import TableAlreadyExists, TableNotFound

from mindsdb.integrations.libs.response import HandlerResponse as Response, RESPONSE_TYPE
from mindsdb.utilities import log


logger = log.getLogger("mindsdb")


class FuncParser:
    def from_string(self, query_string):
        body = py_ast.parse(query_string.strip(), mode="eval").body

        if not isinstance(body, py_ast.Call):
            raise RuntimeError(f"Api function not found {query_string}")

        fnc_name = body.func.id

        params = {}
        for keyword in body.keywords:
            name = keyword.arg
            value = self.process(keyword.value)

            params[name] = value

        return fnc_name, params

    def process(self, node):
        if isinstance(node, py_ast.List):
            elements = []
            for node2 in node.elts:
                elements.append(self.process(node2))
            return elements

        if isinstance(node, py_ast.Dict):
            keys = []
            for node2 in node.keys:
                if isinstance(node2, py_ast.Constant):
                    value = node2.value
                elif isinstance(node2, py_ast.Str):  # py37
                    value = node2.s
                else:
                    raise NotImplementedError(f"Unknown dict key {node2}")

                keys.append(value)

            values = []
            for node2 in node.values:
                values.append(self.process(node2))

            return dict(zip(keys, values))

        if isinstance(node, py_ast.Name):
            # special attributes
            name = node.id
            if name == "true":
                return True
            elif name == "false":
                return False
            elif name == "null":
                return None

        if isinstance(node, py_ast.Constant):
            return node.value

        # ---- python 3.7 objects -----
        if isinstance(node, py_ast.Str):
            return node.s

        if isinstance(node, py_ast.Num):
            return node.n

        # -----------------------------

        if isinstance(node, py_ast.UnaryOp):
            if isinstance(node.op, py_ast.USub):
                value = self.process(node.operand)
                return -value

        raise NotImplementedError(f"Unknown node {node}")


class APITable:
    def __init__(self, handler):
        self.handler = handler

    def select(self, query: Select) -> pd.DataFrame:
        """Receive query as AST (abstract syntax tree) and act upon it.

        Args:
            query (ASTNode): sql query represented as AST. Usually it should be ast.Select

        Returns:
            pd.DataFrame
        """

        raise NotImplementedError()

    def insert(self, query: Insert) -> None:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.

        Args:
            query (ASTNode): sql query represented as AST. Usually it should be ast.Insert

        Returns:
            None
        """
        raise NotImplementedError()

    def update(self, query: ASTNode) -> None:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.

        Args:
            query (ASTNode): sql query represented as AST. Usually it should be ast.Update
        Returns:
            None
        """
        raise NotImplementedError()

    def delete(self, query: ASTNode) -> None:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.

        Args:
            query (ASTNode): sql query represented as AST. Usually it should be ast.Delete

        Returns:
            None
        """
        raise NotImplementedError()

    def get_columns(self) -> list:
        """Maps the columns names from the API call resource

        Returns:
            List
        """
        raise NotImplementedError()


class APIResource(APITable):
    def __init__(self, *args, table_name=None, **kwargs):
        self.table_name = table_name
        super().__init__(*args, **kwargs)

    def select(self, query: Select) -> pd.DataFrame:
        """Receive query as AST (abstract syntax tree) and act upon it.

        Args:
            query (ASTNode): sql query represented as AST. Usually it should be ast.Select

        Returns:
            pd.DataFrame
        """

        conditions = self._extract_conditions(query.where)

        limit = None
        if query.limit:
            limit = query.limit.value

        sort = None
        if query.order_by and len(query.order_by) > 0:
            sort = []
            for an_order in query.order_by:
                sort.append(SortColumn(an_order.field.parts[-1], an_order.direction.upper() != "DESC"))

        targets = []
        for col in query.targets:
            if isinstance(col, Identifier):
                targets.append(col.parts[-1])

        kwargs = {"conditions": conditions, "limit": limit, "sort": sort, "targets": targets}
        if self.table_name is not None:
            kwargs["table_name"] = self.table_name

        result = self.list(**kwargs)

        filters = []
        for cond in conditions:
            if not cond.applied:
                filters.append([cond.op.value, cond.column, cond.value])

        result = filter_dataframe(result, filters)

        if sort:
            sort_columns = []
            for idx, a_sort in enumerate(sort):
                if not a_sort.applied:
                    sort_columns.append(query.order_by[idx])

            result = sort_dataframe(result, sort_columns)

        if limit is not None and len(result) > limit:
            result = result[: int(limit)]

        return result

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
        **kwargs,
    ):
        """
        List items based on specified conditions, limits, sorting, and targets.

        Args:
            conditions (List[FilterCondition]): Optional. A list of conditions to filter the items. Each condition
                                                should be an instance of the FilterCondition class.
            limit (int): Optional. An integer to limit the number of items to be listed.
            sort (List[SortColumn]): Optional. A list of sorting criteria
            targets (List[str]): Optional. A list of strings representing specific fields

        Raises:
            NotImplementedError: This is an abstract method and should be implemented in a subclass.
        """
        raise NotImplementedError()

    def insert(self, query: Insert) -> None:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.

        Args:
            query (ASTNode): sql query represented as AST. Usually it should be ast.Insert

        Returns:
            None
        """

        columns = [col.name for col in query.columns]

        data = [dict(zip(columns, a_row)) for a_row in query.values]
        kwargs = {}
        if self.table_name is not None:
            kwargs["table_name"] = self.table_name

        self.add(data, **kwargs)

    def add(self, row: List[dict], **kwargs) -> None:
        """
        Add a single item to the dataa collection

        Args:
        r   ow (dict): A dictionary representing the item to be added.

        Raises:
            NotImplementedError: This is an abstract method and should be implemented in a subclass.
        """
        raise NotImplementedError()

    def update(self, query: Update) -> None:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.

        Args:
            query (ASTNode): sql query represented as AST. Usually it should be ast.Update

        Returns:
            None
        """
        conditions = self._extract_conditions(query.where)

        values = {key: val.value for key, val in query.update_columns.items()}

        self.modify(conditions, values)

    def modify(self, conditions: List[FilterCondition], values: dict):
        """
        Modify items based on specified conditions and values.

        Args:
            conditions (List[FilterCondition]): A list of conditions to filter the items. Each condition
                                                should be an instance of the FilterCondition class.
            values (dict): A dictionary of values to be updated.

        Raises:
            NotImplementedError: This is an abstract method and should be implemented in a subclass.
        """
        raise NotImplementedError

    def delete(self, query: Delete) -> None:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.

        Args:
            query (ASTNode): sql query represented as AST. Usually it should be ast.Delete

        Returns:
            None
        """
        conditions = self._extract_conditions(query.where)

        self.remove(conditions)

    def remove(self, conditions: List[FilterCondition]):
        """
        Remove items based on specified conditions.

        Args:
            conditions (List[FilterCondition]): A list of conditions to filter the items. Each condition
                                                should be an instance of the FilterCondition class.

        Raises:
            NotImplementedError: This is an abstract method and should be implemented in a subclass.
        """
        raise NotImplementedError()

    def _extract_conditions(self, where: ASTNode) -> List[FilterCondition]:
        return [FilterCondition(i[1], FilterOperator(i[0].upper()), i[2]) for i in extract_comparison_conditions(where)]


class MetaAPIResource(APIResource):
    # TODO: Add a meta_table_info() method in case metadata cannot be retrieved as expected below?

    def meta_get_tables(self, table_name: str, **kwargs) -> dict:
        """
        Retrieves table metadata for the API resource.

        Args:
            table_name (str): The name given to the table that represents the API resource. This is required because the name for the APIResource is given by the handler.
            kwargs: Additional keyword arguments that may be used by the specific API resource implementation.

        Returns:
            Dict: The dictionary should contain the following fields:
            - TABLE_NAME (str): Name of the table.
            - TABLE_TYPE (str): Type of the table, e.g. 'BASE TABLE', 'VIEW', etc. (optional).
            - TABLE_SCHEMA (str): Schema of the table (optional).
            - TABLE_DESCRIPTION (str): Description of the table (optional).
            - ROW_COUNT (int): Estimated number of rows in the table (optional).
        """
        pass

    def meta_get_columns(self, table_name: str, **kwargs) -> List[dict]:
        """
        Retrieves column metadata for the API resource.

        Args:
            table_name (str): The name given to the table that represents the API resource. This is required because the name for the APIResource is given by the handler.
            kwargs: Additional keyword arguments that may be used by the specific API resource implementation.

        Returns:
            List[dict]: The list should contain dictionaries with the following fields:
            - TABLE_NAME (str): Name of the table.
            - COLUMN_NAME (str): Name of the column.
            - DATA_TYPE (str): Data type of the column, e.g. 'VARCHAR', 'INT', etc.
            - COLUMN_DESCRIPTION (str): Description of the column (optional).
            - IS_NULLABLE (bool): Whether the column can contain NULL values (optional).
            - COLUMN_DEFAULT (str): Default value of the column (optional).
        """
        pass

    def meta_get_column_statistics(self, table_name: str, **kwargs) -> List[dict]:
        """
        Retrieves column statistics for the API resource.

        Args:
            table_name (str): The name given to the table that represents the API resource. This is required because the name for the APIResource is given by the handler.
            kwargs: Additional keyword arguments that may be used by the specific API resource implementation.

        Returns:
            List[dict]: The list should contain dictionaries with the following fields:
            - TABLE_NAME (str): Name of the table.
            - COLUMN_NAME (str): Name of the column.
            - MOST_COMMON_VALUES (List[str]): Most common values in the column (optional).
            - MOST_COMMON_FREQUENCIES (List[str]): Frequencies of the most common values in the column (optional).
            - NULL_PERCENTAGE: Percentage of NULL values in the column (optional).
            - MINIMUM_VALUE (str): Minimum value in the column (optional).
            - MAXIMUM_VALUE (str): Maximum value in the column (optional).
            - DISTINCT_VALUES_COUNT (int): Count of distinct values in the column (optional).
        """
        pass

    def meta_get_primary_keys(self, table_name: str, **kwargs) -> List[dict]:
        """
        Retrieves primary key metadata for the API resource.

        Args:
            table_name (str): The name given to the table that represents the API resource. This is required because the name for the APIResource is given by the handler.
            kwargs: Additional keyword arguments that may be used by the specific API resource implementation.

        Returns:
            List[dict]: The list should contain dictionaries with the following fields:
            - TABLE_NAME (str): Name of the table.
            - COLUMN_NAME (str): Name of the column that is part of the primary key.
            - ORDINAL_POSITION (int): Position of the column in the primary key (optional).
            - CONSTRAINT_NAME (str): Name of the primary key constraint (optional).
        """
        pass

    def meta_get_foreign_keys(self, table_name: str, all_tables: List[str], **kwargs) -> List[dict]:
        """
        Retrieves foreign key metadata for the API resource.

        Args:
            table_name (str): The name given to the table that represents the API resource. This is required because the name for the APIResource is given by the handler.
            all_tables (List[str]): A list of all table names in the API resource. This is used to identify relationships between tables.
            kwargs: Additional keyword arguments that may be used by the specific API resource implementation.

        Returns:
            List[dict]: The list should contain dictionaries with the following fields:
            - PARENT_TABLE_NAME (str): Name of the parent table.
            - PARENT_COLUMN_NAME (str): Name of the parent column that is part of the foreign key.
            - CHILD_TABLE_NAME (str): Name of the child table.
            - CHILD_COLUMN_NAME (str): Name of the child column that is part of the foreign key.
            - CONSTRAINT_NAME (str): Name of the foreign key constraint (optional).
        """
        pass


class APIHandler(BaseHandler):
    """
    Base class for handlers associated to the applications APIs (e.g. twitter, slack, discord  etc.)
    """

    def __init__(self, name: str):
        super().__init__(name)
        """ constructor
        Args:
            name (str): the handler name
        """
        self._tables = {}

    def _register_table(self, table_name: str, table_class: Any):
        """
        Register the data resource. For e.g if you are using Twitter API it registers the `tweets` resource from `/api/v2/tweets`.
        """
        if table_name.lower() in self._tables:
            raise TableAlreadyExists(f"Table with name {table_name} already exists for this handler")
        self._tables[table_name.lower()] = table_class

    def _get_table(self, name: Identifier):
        """
        Check if the table name was added to the _register_table
        Args:
            name (Identifier): the table name
        """
        name = name.parts[-1].lower()
        if name in self._tables:
            return self._tables[name]
        raise TableNotFound(f"Table not found: {name}")

    def query(self, query: ASTNode):
        if isinstance(query, Select):
            # If the list method exists, it should be overridden in the child class.
            # The APIResource class could be used as a base class by overriding the select method, but not the list method.
            table = self._get_table(query.from_table)
            list_method = getattr(table, "list", None)
            if not list_method or (list_method and list_method.__func__ is APIResource.list):
                # for back compatibility, targets wasn't passed in previous version
                query.targets = [Star()]
            result = self._get_table(query.from_table).select(query)
        elif isinstance(query, Update):
            result = self._get_table(query.table).update(query)
        elif isinstance(query, Insert):
            result = self._get_table(query.table).insert(query)
        elif isinstance(query, Delete):
            result = self._get_table(query.table).delete(query)
        else:
            raise NotImplementedError

        if result is None:
            return Response(RESPONSE_TYPE.OK)
        elif isinstance(result, pd.DataFrame):
            return Response(RESPONSE_TYPE.TABLE, result)
        else:
            raise NotImplementedError

    def get_columns(self, table_name: str) -> Response:
        """
        Returns a list of entity columns
        Args:
            table_name (str): the table name
        Returns:
            RESPONSE_TYPE.TABLE
        """

        result = self._get_table(Identifier(table_name)).get_columns()

        df = pd.DataFrame(result, columns=["Field"])
        df["Type"] = "str"

        return Response(RESPONSE_TYPE.TABLE, df)

    def get_tables(self) -> Response:
        """
        Return list of entities
        Returns:
            RESPONSE_TYPE.TABLE
        """
        result = list(self._tables.keys())

        df = pd.DataFrame(result, columns=["table_name"])
        df["table_type"] = "BASE TABLE"

        return Response(RESPONSE_TYPE.TABLE, df)


class MetaAPIHandler(APIHandler):
    """
    Base class for handlers associated to the applications APIs (e.g. twitter, slack, discord  etc.)

    This class is used when the handler is also needed to store information in the data catalog.
    """

    def meta_get_handler_info(self, **kwargs) -> str:
        """
        Retrieves information about the design and implementation of the API handler.
        This should include, but not be limited to, the following:
        - The type of SQL queries and operations that the handler supports.
        - etc.

        Args:
            kwargs: Additional keyword arguments that may be used in generating the handler information.

        Returns:
            str: A string containing information about the API handler's design and implementation.
        """
        pass

    def meta_get_tables(self, table_names: Optional[List[str]] = None, **kwargs) -> Response:
        """
        Retrieves metadata for the specified tables (or all tables if no list is provided).

        Args:
            table_names (List): A list of table names for which to retrieve metadata.
            kwargs: Additional keyword arguments that may be used by the specific API resource implementation.

        Returns:
            Response: A response object containing the table metadata.
        """
        df = pd.DataFrame()
        for table_name, table_class in self._tables.items():
            if table_names is None or table_name in table_names:
                try:
                    if hasattr(table_class, "meta_get_tables"):
                        table_metadata = table_class.meta_get_tables(table_name, **kwargs)
                        df = pd.concat([df, pd.DataFrame([table_metadata])], ignore_index=True)
                except Exception as e:
                    logger.error(f"Error retrieving metadata for table {table_name}: {e}")

        return Response(RESPONSE_TYPE.TABLE, df)

    def meta_get_columns(self, table_names: Optional[List[str]] = None, **kwargs) -> Response:
        """
        Retrieves column metadata for the specified tables (or all tables if no list is provided).

        Args:
            table_names (List): A list of table names for which to retrieve column metadata.

        Returns:
            Response: A response object containing the column metadata.
        """
        df = pd.DataFrame()
        for table_name, table_class in self._tables.items():
            if table_names is None or table_name in table_names:
                try:
                    if hasattr(table_class, "meta_get_columns"):
                        column_metadata = table_class.meta_get_columns(table_name, **kwargs)
                        df = pd.concat([df, pd.DataFrame(column_metadata)], ignore_index=True)
                except Exception as e:
                    logger.error(f"Error retrieving column metadata for table {table_name}: {e}")

        return Response(RESPONSE_TYPE.TABLE, df)

    def meta_get_column_statistics(self, table_names: Optional[List[str]] = None, **kwargs) -> Response:
        """
        Retrieves column statistics for the specified tables (or all tables if no list is provided).

        Args:
            table_names (List): A list of table names for which to retrieve column statistics.

        Returns:
            Response: A response object containing the column statistics.
        """
        df = pd.DataFrame()
        for table_name, table_class in self._tables.items():
            if table_names is None or table_name in table_names:
                try:
                    if hasattr(table_class, "meta_get_column_statistics"):
                        column_statistics = table_class.meta_get_column_statistics(table_name, **kwargs)
                        df = pd.concat([df, pd.DataFrame(column_statistics)], ignore_index=True)
                except Exception as e:
                    logger.error(f"Error retrieving column statistics for table {table_name}: {e}")

        return Response(RESPONSE_TYPE.TABLE, df)

    def meta_get_primary_keys(self, table_names: Optional[List[str]] = None, **kwargs) -> Response:
        """
        Retrieves primary key metadata for the specified tables (or all tables if no list is provided).

        Args:
            table_names (List): A list of table names for which to retrieve primary key metadata.

        Returns:
            Response: A response object containing the primary key metadata.
        """
        df = pd.DataFrame()
        for table_name, table_class in self._tables.items():
            if table_names is None or table_name in table_names:
                try:
                    if hasattr(table_class, "meta_get_primary_keys"):
                        primary_key_metadata = table_class.meta_get_primary_keys(table_name, **kwargs)
                        df = pd.concat([df, pd.DataFrame(primary_key_metadata)], ignore_index=True)
                except Exception as e:
                    logger.error(f"Error retrieving primary keys for table {table_name}: {e}")

        return Response(RESPONSE_TYPE.TABLE, df)

    def meta_get_foreign_keys(self, table_names: Optional[List[str]] = None, **kwargs) -> Response:
        """
        Retrieves foreign key metadata for the specified tables (or all tables if no list is provided).

        Args:
            table_names (List): A list of table names for which to retrieve foreign key metadata.

        Returns:
            Response: A response object containing the foreign key metadata.
        """
        df = pd.DataFrame()
        all_tables = list(self._tables.keys())
        for table_name, table_class in self._tables.items():
            if table_names is None or table_name in table_names:
                try:
                    if hasattr(table_class, "meta_get_foreign_keys"):
                        foreign_key_metadata = table_class.meta_get_foreign_keys(
                            table_name, all_tables=table_names if table_names else all_tables, **kwargs
                        )
                        df = pd.concat([df, pd.DataFrame(foreign_key_metadata)], ignore_index=True)
                except Exception as e:
                    logger.error(f"Error retrieving foreign keys for table {table_name}: {e}")

        return Response(RESPONSE_TYPE.TABLE, df)


class APIChatHandler(APIHandler):
    def get_chat_config(self):
        """Return configuration to connect to chatbot

        Returns:
            Dict
        """
        raise NotImplementedError()

    def get_my_user_name(self) -> list:
        """Return configuration to connect to chatbot

        Returns:
            Dict
        """
        raise NotImplementedError()
