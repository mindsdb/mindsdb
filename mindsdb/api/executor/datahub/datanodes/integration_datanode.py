import time
import inspect
import functools
from dataclasses import astuple

import pandas as pd
from sqlalchemy.types import Integer, Float

from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb_sql_parser.ast import Insert, Identifier, CreateTable, TableColumn, DropTables

from mindsdb.api.executor.datahub.datanodes.datanode import DataNode
from mindsdb.api.executor.datahub.datanodes.system_tables import infer_mysql_type
from mindsdb.api.executor.datahub.classes.tables_row import TablesRow
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.api.executor.sql_query.result_set import ResultSet
from mindsdb.integrations.libs.response import INF_SCHEMA_COLUMNS_NAMES, DataHandlerResponse, ErrorResponse, OkResponse
from mindsdb.integrations.utilities.utils import get_class_name
from mindsdb.metrics import metrics
from mindsdb.utilities import log
from mindsdb.utilities.profiler import profiler
from mindsdb.utilities.exception import QueryError

logger = log.getLogger(__name__)


class DBHandlerException(Exception):
    pass


def collect_metrics(func):
    """Decorator for collecting performance metrics if integration handler query.

    The decorator measures:
    - Query execution time using high-precision performance counter
    - Response size (number of rows returned)

    Args:
        func: The function to be decorated (integration handler method)

    Returns:
        function: Wrapped function that includes metrics collection and error handling
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            time_before_query = time.perf_counter()
            result = func(self, *args, **kwargs)

            # metrics
            handler_class_name = get_class_name(self.integration_handler)
            elapsed_seconds = time.perf_counter() - time_before_query
            query_time_with_labels = metrics.INTEGRATION_HANDLER_QUERY_TIME.labels(handler_class_name, result.type)
            query_time_with_labels.observe(elapsed_seconds)

            num_rows = getattr(result, "affected_rows", None)
            if num_rows is None:
                num_rows = -1
            response_size_with_labels = metrics.INTEGRATION_HANDLER_RESPONSE_SIZE.labels(
                handler_class_name, result.type
            )
            response_size_with_labels.observe(num_rows)
            logger.debug(f"Handler '{handler_class_name}' returned {num_rows} rows in {elapsed_seconds:.3f} seconds")
        except Exception as e:
            msg = str(e).strip()
            if msg == "":
                msg = e.__class__.__name__
            msg = f"[{self.ds_type}/{self.integration_name}]: {msg}"
            raise DBHandlerException(msg) from e
        return result

    return wrapper


class IntegrationDataNode(DataNode):
    type = "integration"

    def __init__(self, integration_name, ds_type, integration_controller):
        self.integration_name = integration_name
        self.ds_type = ds_type
        self.integration_controller = integration_controller
        self.integration_handler = self.integration_controller.get_data_handler(self.integration_name)

    def get_type(self):
        return self.type

    def get_tables(self):
        response = self.integration_handler.get_tables()
        if response.type == RESPONSE_TYPE.TABLE:
            result_dict = response.data_frame.to_dict(orient="records")
            return [TablesRow.from_dict(row) for row in result_dict]
        else:
            raise Exception(f"Can't get tables: {response.error_message}")

    def get_table_columns_df(self, table_name: str, schema_name: str | None = None) -> pd.DataFrame:
        """Get a DataFrame containing representation of information_schema.columns for the specified table.

        Args:
            table_name (str): The name of the table to get columns from.
            schema_name (str | None): The name of the schema to get columns from.

        Returns:
            pd.DataFrame: A DataFrame containing representation of information_schema.columns for the specified table.
                          The DataFrame has list of columns as in the integrations.libs.response.INF_SCHEMA_COLUMNS_NAMES.
        """
        if "schema_name" in inspect.signature(self.integration_handler.get_columns).parameters:
            response = self.integration_handler.get_columns(table_name, schema_name)
        else:
            response = self.integration_handler.get_columns(table_name)

        if response.type == RESPONSE_TYPE.COLUMNS_TABLE:
            return response.data_frame

        if response.type != RESPONSE_TYPE.TABLE:
            logger.warning(f"Wrong response type for handler's `get_columns` call: {response.type}")
            return pd.DataFrame([], columns=astuple(INF_SCHEMA_COLUMNS_NAMES))

        # region fallback for old handlers
        df = response.data_frame
        df.columns = [name.upper() for name in df.columns]
        if "FIELD" not in df.columns or "TYPE" not in df.columns:
            logger.warning(
                f"Response from the handler's `get_columns` call does not contain required columns: {list(df.columns)}"
            )
            return pd.DataFrame([], columns=astuple(INF_SCHEMA_COLUMNS_NAMES))

        new_df = df[["FIELD", "TYPE"]]
        new_df.columns = ["COLUMN_NAME", "DATA_TYPE"]

        new_df[INF_SCHEMA_COLUMNS_NAMES.MYSQL_DATA_TYPE] = new_df[INF_SCHEMA_COLUMNS_NAMES.DATA_TYPE].apply(
            lambda x: infer_mysql_type(x).value
        )

        for column_name in astuple(INF_SCHEMA_COLUMNS_NAMES):
            if column_name in new_df.columns:
                continue
            new_df[column_name] = None
        # endregion

        return new_df

    def get_table_columns_names(self, table_name: str, schema_name: str | None = None) -> list[str]:
        """Get a list of column names for the specified table.

        Args:
            table_name (str): The name of the table to get columns from.
            schema_name (str | None): The name of the schema to get columns from.

        Returns:
            list[str]: A list of column names for the specified table.
        """
        df = self.get_table_columns_df(table_name, schema_name)
        return df[INF_SCHEMA_COLUMNS_NAMES.COLUMN_NAME].to_list()

    def drop_table(self, name: Identifier, if_exists=False):
        drop_ast = DropTables(tables=[name], if_exists=if_exists)
        self.query(drop_ast)

    def create_table(
        self,
        table_name: Identifier,
        result_set: ResultSet = None,
        columns: list[TableColumn] = None,
        is_replace: bool = False,
        is_create: bool = False,
        raise_if_exists: bool = True,
        **kwargs,
    ) -> OkResponse:
        # is_create - create table
        #   if !raise_if_exists: error will be skipped
        # is_replace - drop table if exists
        # is_create==False and is_replace==False: just insert

        table_columns_meta = {}

        if columns is None:
            columns: list[TableColumn] = result_set.get_ast_columns()
            table_columns_meta = {column.name: column.type for column in columns}

        if is_replace:
            # drop
            drop_ast = DropTables(tables=[table_name], if_exists=True)
            self.query(drop_ast)
            is_create = True

        if is_create:
            create_table_ast = CreateTable(name=table_name, columns=columns, is_replace=is_replace)
            try:
                self.query(create_table_ast)
            except Exception as e:
                if raise_if_exists:
                    raise e

        if result_set is None:
            # it is just a 'create table'
            return OkResponse()

        # native insert
        if hasattr(self.integration_handler, "insert"):
            df = result_set.to_df()

            result: DataHandlerResponse = self.integration_handler.insert(table_name.parts[-1], df)
            if result is not None:
                affected_rows = result.affected_rows
            else:
                affected_rows = None
            return OkResponse(affected_rows=affected_rows)

        insert_columns = [Identifier(parts=[x.alias]) for x in result_set.columns]

        # adapt table types
        for col_idx, col in enumerate(result_set.columns):
            column_type = table_columns_meta[col.alias]

            if column_type == Integer:
                type_name = "int"
            elif column_type == Float:
                type_name = "float"
            else:
                continue

            try:
                result_set.set_col_type(col_idx, type_name)
            except Exception:
                pass

        values = result_set.to_lists()

        if len(values) == 0:
            # not need to insert
            return OkResponse()

        insert_ast = Insert(table=table_name, columns=insert_columns, values=values, is_plain=True)

        try:
            result: DataHandlerResponse = self.query(insert_ast)
        except Exception as e:
            msg = f"[{self.ds_type}/{self.integration_name}]: {str(e)}"
            raise DBHandlerException(msg) from e

        return OkResponse(affected_rows=result.affected_rows)

    def has_support_stream(self) -> bool:
        return getattr(self.integration_handler, "stream_response", False)

    @profiler.profile()
    def query(self, query: ASTNode | str = None, session=None) -> DataHandlerResponse:
        """Execute a query against the integration data source.

        This method processes SQL queries either as ASTNode objects or raw SQL strings

        Args:
            query (ASTNode | str, optional): The query to execute. Can be either:
                - ASTNode: A parsed SQL query object
                - str: Raw SQL query string
            session: Session object (currently unused but kept for compatibility)

        Returns:
            DataHandlerResponse: Response object

        Raises:
            NotImplementedError: If query is not ASTNode or str type
            Exception: If the query execution fails with an error response
        """
        if isinstance(query, ASTNode):
            result: DataHandlerResponse = self.query_integration_handler(query=query)
        elif isinstance(query, str):
            result: DataHandlerResponse = self.native_query_integration(query=query)
        else:
            raise NotImplementedError("Thew query argument must be ASTNode or string type")

        if type(result) is ErrorResponse:
            if isinstance(query, ASTNode):
                try:
                    query_str = query.to_string()
                except Exception:
                    # most likely it is CreateTable with exotic column types
                    query_str = "can't be dump"
            else:
                query_str = query

            exception = QueryError(
                db_name=self.integration_handler.name,
                db_type=self.integration_handler.__class__.name,
                db_error_msg=result.error_message,
                failed_query=query_str,
                is_expected=result.is_expected_error,
            )

            if result.exception is None:
                raise exception
            else:
                raise exception from result.exception

        return result

    @collect_metrics
    def query_integration_handler(self, query: ASTNode) -> DataHandlerResponse:
        return self.integration_handler.query(query)

    @collect_metrics
    def native_query_integration(self, query: str) -> DataHandlerResponse:
        return self.integration_handler.native_query(query)
