import ast
import concurrent.futures
import inspect
import textwrap
from _ast import AnnAssign, AugAssign
from typing import Any, Dict, List, Optional

import pandas as pd
from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb.utilities import log

from mindsdb.integrations.libs.response import HandlerResponse, HandlerStatusResponse, RESPONSE_TYPE

logger = log.getLogger(__name__)


class BaseHandler:
    """Base class for database handlers

    Base class for handlers that associate a source of information with the
    broader MindsDB ecosystem via SQL commands.
    """

    def __init__(self, name: str):
        """constructor
        Args:
            name (str): the handler name
        """
        self.is_connected: bool = False
        self.name = name

    def connect(self):
        """Set up any connections required by the handler

        Should return connection

        """
        raise NotImplementedError()

    def disconnect(self):
        """Close any existing connections

        Should switch self.is_connected.
        """
        self.is_connected = False
        return

    def check_connection(self) -> HandlerStatusResponse:
        """Check connection to the handler

        Returns:
            HandlerStatusResponse
        """
        raise NotImplementedError()

    def native_query(self, query: Any) -> HandlerResponse:
        """Receive raw query and act upon it somehow.

        Args:
            query (Any): query in native format (str for sql databases,
                dict for mongo, etc)

        Returns:
            HandlerResponse
        """
        raise NotImplementedError()

    def query(self, query: ASTNode) -> HandlerResponse:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.

        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INSERT, DELETE, etc

        Returns:
            HandlerResponse
        """
        raise NotImplementedError()

    def get_tables(self) -> HandlerResponse:
        """Return list of entities

        Return list of entities that will be accesible as tables.

        Returns:
            HandlerResponse: shoud have same columns as information_schema.tables
                (https://dev.mysql.com/doc/refman/8.0/en/information-schema-tables-table.html)
                Column 'TABLE_NAME' is mandatory, other is optional.
        """
        raise NotImplementedError()

    def get_columns(self, table_name: str) -> HandlerResponse:
        """Returns a list of entity columns

        Args:
            table_name (str): name of one of tables returned by self.get_tables()

        Returns:
            HandlerResponse: shoud have same columns as information_schema.columns
                (https://dev.mysql.com/doc/refman/8.0/en/information-schema-columns-table.html)
                Column 'COLUMN_NAME' is mandatory, other is optional. Hightly
                recomended to define also 'DATA_TYPE': it should be one of
                python data types (by default it str).
        """
        raise NotImplementedError()


class DatabaseHandler(BaseHandler):
    """
    Base class for handlers associated to data storage systems (e.g. databases, data warehouses, streaming services, etc.)
    """

    def __init__(self, name: str):
        super().__init__(name)


class MetaDatabaseHandler(DatabaseHandler):
    """
    Base class for handlers associated to data storage systems (e.g. databases, data warehouses, streaming services, etc.)

    This class is used when the handler is also needed to store information in the data catalog.
    This information is typically avaiable in the information schema or system tables of the database.
    """

    def __init__(self, name: str):
        super().__init__(name)

    def meta_get_tables(self, table_names: Optional[List[str]]) -> HandlerResponse:
        """
        Returns metadata information about the tables to be stored in the data catalog.

        Returns:
            HandlerResponse: The response should consist of the following columns:
            - TABLE_NAME (str): Name of the table.
            - TABLE_TYPE (str): Type of the table, e.g. 'BASE TABLE', 'VIEW', etc. (optional).
            - TABLE_SCHEMA (str): Schema of the table (optional).
            - TABLE_DESCRIPTION (str): Description of the table (optional).
            - ROW_COUNT (int): Estimated number of rows in the table (optional).
        """
        raise NotImplementedError()

    def meta_get_columns(self, table_names: Optional[List[str]]) -> HandlerResponse:
        """
        Returns metadata information about the columns in the tables to be stored in the data catalog.

        Returns:
            HandlerResponse: The response should consist of the following columns:
            - TABLE_NAME (str): Name of the table.
            - COLUMN_NAME (str): Name of the column.
            - DATA_TYPE (str): Data type of the column, e.g. 'VARCHAR', 'INT', etc.
            - COLUMN_DESCRIPTION (str): Description of the column (optional).
            - IS_NULLABLE (bool): Whether the column can contain NULL values (optional).
            - COLUMN_DEFAULT (str): Default value of the column (optional).
        """
        raise NotImplementedError()

    def meta_get_column_statistics(self, table_names: Optional[List[str]]) -> HandlerResponse:
        """
        Returns metadata statisical information about the columns in the tables to be stored in the data catalog.
        Either this method should be overridden in the handler or `meta_get_column_statistics_for_table` should be implemented.

        Returns:
            HandlerResponse: The response should consist of the following columns:
            - TABLE_NAME (str): Name of the table.
            - COLUMN_NAME (str): Name of the column.
            - MOST_COMMON_VALUES (List[str]): Most common values in the column (optional).
            - MOST_COMMON_FREQUENCIES (List[str]): Frequencies of the most common values in the column (optional).
            - NULL_PERCENTAGE: Percentage of NULL values in the column (optional).
            - MINIMUM_VALUE (str): Minimum value in the column (optional).
            - MAXIMUM_VALUE (str): Maximum value in the column (optional).
            - DISTINCT_VALUES_COUNT (int): Count of distinct values in the column (optional).
        """
        method = getattr(self, "meta_get_column_statistics_for_table")
        if method.__func__ is not MetaDatabaseHandler.meta_get_column_statistics_for_table:
            meta_columns = self.meta_get_columns(table_names)
            grouped_columns = (
                meta_columns.data_frame.groupby("table_name")
                .agg(
                    {
                        "column_name": list,
                    }
                )
                .reset_index()
            )

            executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
            futures = []

            results = []
            with executor:
                for _, row in grouped_columns.iterrows():
                    table_name = row["table_name"]
                    columns = row["column_name"]
                    futures.append(executor.submit(self.meta_get_column_statistics_for_table, table_name, columns))

            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result(timeout=120)
                    if result.resp_type == RESPONSE_TYPE.TABLE:
                        results.append(result.data_frame)
                    else:
                        logger.error(
                            f"Error retrieving column statistics for table {table_name}: {result.error_message}"
                        )
                except Exception as e:
                    logger.error(f"Exception occurred while retrieving column statistics for table {table_name}: {e}")

            if not results:
                logger.warning("No column statistics could be retrieved for the specified tables.")
                return HandlerResponse(RESPONSE_TYPE.ERROR, error_message="No column statistics could be retrieved.")
            return HandlerResponse(
                RESPONSE_TYPE.TABLE, pd.concat(results, ignore_index=True) if results else pd.DataFrame()
            )

        else:
            raise NotImplementedError()

    def meta_get_column_statistics_for_table(
        self, table_name: str, column_names: Optional[List[str]] = None
    ) -> HandlerResponse:
        """
        Returns metadata statistical information about the columns in a specific table to be stored in the data catalog.
        Either this method should be implemented in the handler or `meta_get_column_statistics` should be overridden.

        Args:
            table_name (str): Name of the table.
            column_names (Optional[List[str]]): List of column names to retrieve statistics for. If None, statistics for all columns will be returned.

        Returns:
            HandlerResponse: The response should consist of the following columns:
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

    def meta_get_primary_keys(self, table_names: Optional[List[str]]) -> HandlerResponse:
        """
        Returns metadata information about the primary keys in the tables to be stored in the data catalog.

        Returns:
            HandlerResponse: The response should consist of the following columns:
            - TABLE_NAME (str): Name of the table.
            - COLUMN_NAME (str): Name of the column that is part of the primary key.
            - ORDINAL_POSITION (int): Position of the column in the primary key (optional).
            - CONSTRAINT_NAME (str): Name of the primary key constraint (optional).
        """
        raise NotImplementedError()

    def meta_get_foreign_keys(self, table_names: Optional[List[str]]) -> HandlerResponse:
        """
        Returns metadata information about the foreign keys in the tables to be stored in the data catalog.

        Returns:
            HandlerResponse: The response should consist of the following columns:
            - PARENT_TABLE_NAME (str): Name of the parent table.
            - PARENT_COLUMN_NAME (str): Name of the parent column that is part of the foreign key.
            - CHILD_TABLE_NAME (str): Name of the child table.
            - CHILD_COLUMN_NAME (str): Name of the child column that is part of the foreign key.
            - CONSTRAINT_NAME (str): Name of the foreign key constraint (optional).
        """
        raise NotImplementedError()

    def meta_get_handler_info(self, **kwargs) -> str:
        """
        Retrieves information about the design and implementation of the database handler.
        This should include, but not be limited to, the following:
        - The type of SQL queries and operations that the handler supports.
        - etc.

        Args:
            kwargs: Additional keyword arguments that may be used in generating the handler information.

        Returns:
            str: A string containing information about the database handler's design and implementation.
        """
        pass


class ArgProbeMixin:
    """
    A mixin class that provides probing of arguments that
    are needed by a handler during creation and prediction time
    by running the static analysis on the source code of the handler.
    """

    class ArgProbeVisitor(ast.NodeVisitor):
        def __init__(self):
            self.arg_keys = []
            self.var_names_to_track = {"args"}

        def visit_Assign(self, node):
            # track if args['using'] get assigned to any variable
            # if so, we should track the variable by adding it to
            # self.var_names_to_track
            # E.g., using_args = args['using']
            # we should track using_args as well
            if (
                isinstance(node.value, ast.Subscript)
                and isinstance(node.value.value, ast.Name)
                and node.value.value.id == "args"
            ):
                if (
                    isinstance(node.value.slice, ast.Index)
                    and isinstance(node.value.slice.value, ast.Str)
                    and node.value.slice.value.s == "using"
                ):
                    self.var_names_to_track.add(node.targets[0].id)

            # for an assignment like `self.args['name'] = 'value'`, we should ignore
            # the left side of the assignment
            self.visit(node.value)

        def visit_AnnAssign(self, node: AnnAssign) -> Any:
            self.visit(node.value)

        def visit_AugAssign(self, node: AugAssign) -> Any:
            self.visit(node.value)

        def visit_Subscript(self, node):
            if isinstance(node.value, ast.Name) and node.value.id in self.var_names_to_track:
                if isinstance(node.slice, ast.Index) and isinstance(node.slice.value, ast.Str):
                    self.arg_keys.append({"name": node.slice.value.s, "required": True})
            self.generic_visit(node)

        def visit_Call(self, node):
            if isinstance(node.func, ast.Attribute) and node.func.attr == "get":
                if isinstance(node.func.value, ast.Name) and node.func.value.id in self.var_names_to_track:
                    if isinstance(node.args[0], ast.Str):
                        self.arg_keys.append({"name": node.args[0].s, "required": False})
            self.generic_visit(node)

    @classmethod
    def probe_function(self, method_name: str) -> List[Dict]:
        """
        Probe the source code of the method with name method_name.
        Specifically, trace how the argument `args`, which is a dict is used in the method.

        Find all places where a key of the dict is used, and return a list of all keys that are used.
        E.g.,
        args["key1"] -> "key1" is accessed, and it is required
        args.get("key2", "default_value") -> "key2" is accessed, and it is optional (default value is provided)

        Return a list of dict
        where each dict looks like
        {
            "name": "key1",
            "required": True
        }
        """
        try:
            source_code = self.get_source_code(method_name)
        except Exception as e:
            logger.error(f"Failed to get source code of method {method_name} in {self.__class__.__name__}. Reason: {e}")
            return []

        # parse the source code
        # fix the indentation
        source_code = textwrap.dedent(source_code)
        # parse the source code
        tree = ast.parse(source_code)

        # find all places where a key in args is accessed
        # and if it is accessed using args["key"] or args.get("key", "default_value")

        visitor = self.ArgProbeVisitor()
        visitor.visit(tree)

        # deduplicate the keys
        # if there two records with the same name but different required status
        # we should keep the one with required == True
        unique_arg_keys = {}
        for r in visitor.arg_keys:
            if r["name"] in unique_arg_keys:
                if r["required"]:
                    unique_arg_keys[r["name"]] = r["required"]
            else:
                unique_arg_keys[r["name"]] = r["required"]

        # convert back to list
        visitor.arg_keys = [{"name": k, "required": v} for k, v in unique_arg_keys.items()]

        # filter out record where name == "using"
        return [r for r in visitor.arg_keys if r["name"] != "using"]

    @classmethod
    def get_source_code(self, method_name: str):
        """
        Get the source code of the method specified by method_name
        """
        method = getattr(self, method_name)
        if method is None:
            raise Exception(f"Method {method_name} does not exist in {self.__class__.__name__}")
        source_code = inspect.getsource(method)
        return source_code

    @classmethod
    def prediction_args(self):
        """
        Get the arguments that are needed by the prediction method
        """
        return self.probe_function("predict")

    @classmethod
    def creation_args(self):
        """
        Get the arguments that are needed by the creation method
        """
        return self.probe_function("create")


class BaseMLEngine(ArgProbeMixin):
    """
    Base class for integration engines to connect with other machine learning libraries/frameworks.

    This class will be instanced when interacting with the underlying framework. For compliance with the interface
    that MindsDB core expects, instances of this class will be wrapped with the `BaseMLEngineExec` class defined
    in `libs/ml_exec_base`.

    Broadly speaking, the flow is as follows:
      - A SQL statement is sent to the MindsDB executor
      - The statement is parsed, and a sequential plan is generated by `mindsdb_sql`
      - If any step in the plan involves an ML framework, a wrapped engine that inherits from this class will be called for the respective action
          - For example, creating a new model would call `create()`
      - Any output produced by the ML engine is then formatted by the wrapper and passed back into the MindsDB executor, which can then morph the data to comply with the original SQL query
    """  # noqa

    def __init__(self, model_storage, engine_storage, **kwargs) -> None:
        """
        Warning: This method should not be overridden.

        Initialize storage objects required by the ML engine.

        - engine_storage: persists global engine-related internals or artifacts that may be used by all models from the engine.
        - model_storage: stores artifacts for any single given model.
        """
        self.model_storage = model_storage
        self.engine_storage = engine_storage
        self.generative = False  # if True, the target column name does not have to be specified at creation time

        if kwargs.get("base_model_storage"):
            self.base_model_storage = kwargs["base_model_storage"]  # available when updating a model
        else:
            self.base_model_storage = None

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        """
        Saves a model inside the engine registry for later usage.

        Normally, an input dataframe is required to train the model.
        However, some integrations may merely require registering the model instead of training, in which case `df` can be omitted.

        Any other arguments required to register the model can be passed in an `args` dictionary.
        """
        raise NotImplementedError

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        """
        Calls a model with some input dataframe `df`, and optionally some arguments `args` that may modify the model behavior.

        The expected output is a dataframe with the predicted values in the target-named column.
        Additional columns can be present, and will be considered row-wise explanations if their names finish with `_explain`.
        """
        raise NotImplementedError

    def finetune(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        """
        Optional.

        Used to fine-tune a pre-existing model without resetting its internal state (e.g. weights).

        Availability will depend on underlying integration support, as not all ML models can be partially updated.
        """
        raise NotImplementedError

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        """Optional.

        When called, this method provides global model insights, e.g. framework-level parameters used in training.
        """
        raise NotImplementedError

    def update(self, args: dict) -> None:
        """Optional.

        Update model.
        """
        raise NotImplementedError

    def create_engine(self, connection_args: dict):
        """Optional.

        Used to connect with external sources (e.g. a REST API) that the engine will require to use any other methods.
        """
        raise NotImplementedError

    def update_engine(self, connection_args: dict):
        """Optional.

        Used when need to change connection args or do any make any other changes to the engine
        """
        raise NotImplementedError

    def close(self):
        pass
