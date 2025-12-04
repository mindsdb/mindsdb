from typing import Text, Dict, Any, Optional

import pandas as pd
from databricks.sql import connect, RequestError, ServerOperationError
from databricks.sql.client import Connection
from databricks.sqlalchemy import DatabricksDialect
from mindsdb_sql_parser.ast.base import ASTNode

from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
    INF_SCHEMA_COLUMNS_NAMES_SET,
)
from mindsdb.utilities import log
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE


logger = log.getLogger(__name__)


def _map_type(internal_type_name: str | None) -> MYSQL_DATA_TYPE:
    """Map MyDatabricks SQL text types names to MySQL types as enum.

    Args:
        internal_type_name (str): The name of the Databricks type to map.

    Returns:
        MYSQL_DATA_TYPE: The MySQL type enum that corresponds to the MySQL text type name.
    """
    if not isinstance(internal_type_name, str):
        return MYSQL_DATA_TYPE.TEXT
    if internal_type_name.upper() == "STRING":
        return MYSQL_DATA_TYPE.TEXT
    if internal_type_name.upper() == "LONG":
        return MYSQL_DATA_TYPE.BIGINT
    if internal_type_name.upper() == "SHORT":
        return MYSQL_DATA_TYPE.SMALLINT
    try:
        return MYSQL_DATA_TYPE(internal_type_name.upper())
    except Exception:
        logger.info(f"Databricks handler: unknown type: {internal_type_name}, use TEXT as fallback.")
        return MYSQL_DATA_TYPE.TEXT


class DatabricksHandler(DatabaseHandler):
    """
    This handler handles the connection and execution of SQL statements on Databricks.
    """

    name = "databricks"

    def __init__(self, name: Text, connection_data: Optional[Dict], **kwargs: Any) -> None:
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the Databricks workspace.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False
        self.thread_safe = True

    def __del__(self) -> None:
        """
        Closes the connection when the handler instance is deleted.
        """
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> Connection:
        """
        Establishes a connection to the Databricks workspace.

        Raises:
            ValueError: If the expected connection parameters are not provided.

        Returns:
            databricks.sql.client.Connection: A connection object to the Databricks workspace.
        """
        if self.is_connected is True:
            return self.connection

        # Mandatory connection parameters.
        if not all(key in self.connection_data for key in ["server_hostname", "http_path", "access_token"]):
            raise ValueError("Required parameters (server_hostname, http_path, access_token) must be provided.")

        config = {
            "server_hostname": self.connection_data["server_hostname"],
            "http_path": self.connection_data["http_path"],
            "access_token": self.connection_data["access_token"],
        }

        # Optional connection parameters.
        optional_parameters = [
            "session_configuration",
            "http_headers",
            "catalog",
            "schema",
        ]
        for parameter in optional_parameters:
            if parameter in self.connection_data:
                config[parameter] = self.connection_data[parameter]

        try:
            self.connection = connect(**config)
            self.is_connected = True
            return self.connection
        except RequestError as request_error:
            logger.error(f"Request error when connecting to Databricks: {request_error}")
            raise
        except RuntimeError as runtime_error:
            logger.error(f"Runtime error when connecting to Databricks: {runtime_error}")
            raise
        except Exception as unknown_error:
            logger.error(f"Unknown error when connecting to Databricks: {unknown_error}")
            raise

    def disconnect(self):
        """
        Closes the connection to the Databricks workspace if it's currently open.
        """
        if self.is_connected is False:
            return

        self.connection.close()
        self.is_connected = False
        return self.is_connected

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Databricks workspace.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()

            # Execute a simple query to check the connection.
            query = "SELECT 1 FROM information_schema.schemata"
            if "schema" in self.connection_data:
                query += f" WHERE schema_name = '{self.connection_data['schema']}'"

            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()

            # If the query does not return a result, the schema does not exist.
            if not result:
                raise ValueError(f"The schema {self.connection_data['schema']} does not exist!")

            response.success = True
        except (ValueError, RequestError, RuntimeError, ServerOperationError) as known_error:
            logger.error(f"Connection check to Databricks failed, {known_error}!")
            response.error_message = str(known_error)
        except Exception as unknown_error:
            logger.error(f"Connection check to Databricks failed due to an unknown error, {unknown_error}!")
            response.error_message = str(unknown_error)

        if response.success and need_to_close:
            self.disconnect()

        elif not response.success and self.is_connected:
            self.is_connected = False

        return response

    def native_query(self, query: Text) -> Response:
        """
        Executes a native SQL query on the Databricks workspace and returns the result.

        Args:
            query (Text): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        need_to_close = self.is_connected is False

        connection = self.connect()
        with connection.cursor() as cursor:
            try:
                cursor.execute(query)
                result = cursor.fetchall()
                if result:
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        data_frame=pd.DataFrame(result, columns=[x[0] for x in cursor.description]),
                    )

                else:
                    response = Response(RESPONSE_TYPE.OK)
                    connection.commit()
            except ServerOperationError as server_error:
                logger.error(f"Server error running query: {query} on Databricks, {server_error}!")
                response = Response(RESPONSE_TYPE.ERROR, error_message=str(server_error))
            except Exception as unknown_error:
                logger.error(f"Unknown error running query: {query} on Databricks, {unknown_error}!")
                response = Response(RESPONSE_TYPE.ERROR, error_message=str(unknown_error))

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Executes a SQL query represented by an ASTNode on the Databricks Workspace and retrieves the data.

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.

        Returns:
            Response: The response from the `native_query` method, containing the result of the SQL query execution.
        """
        renderer = SqlalchemyRender(DatabricksDialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self, all: bool = False) -> Response:
        """
        Retrieves a list of all non-system tables in the connected schema of the Databricks workspace.

        Args:
            all (bool): If True - return tables from all schemas.

        Returns:
            Response: A response object containing a list of tables in the connected schema.
        """
        all_filter = "and table_schema = current_schema()"
        if all is True:
            all_filter = ""
        query = f"""
            SELECT
                table_schema,
                table_name,
                table_type
            FROM
                information_schema.tables
            WHERE
                table_schema != 'information_schema'
                {all_filter}
        """
        result = self.native_query(query)
        if result.resp_type == RESPONSE_TYPE.OK:
            result = Response(
                RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame([], columns=list(INF_SCHEMA_COLUMNS_NAMES_SET))
            )
        return result

    def get_columns(self, table_name: str, schema_name: str | None = None) -> Response:
        """
        Retrieves column details for a specified table in the Databricks workspace.

        Args:
            table_name (str): The name of the table for which to retrieve column information.
            schema_name (str|None): The name of the schema in which the table is located.

        Raises:
            ValueError: If the 'table_name' is not a valid string.

        Returns:
            Response: A response object containing the column details.
        """
        if not table_name or not isinstance(table_name, str):
            raise ValueError("Invalid table name provided.")

        if isinstance(schema_name, str):
            schema_name = f"'{schema_name}'"
        else:
            schema_name = "current_schema()"
        query = f"""
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                ORDINAL_POSITION,
                COLUMN_DEFAULT,
                IS_NULLABLE,
                CHARACTER_MAXIMUM_LENGTH,
                CHARACTER_OCTET_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                DATETIME_PRECISION,
                null as CHARACTER_SET_NAME,
                null as COLLATION_NAME
            FROM
                information_schema.columns
            WHERE
                table_name = '{table_name}'
            AND
                table_schema = {schema_name}
        """

        result = self.native_query(query)
        if result.resp_type == RESPONSE_TYPE.OK:
            result = Response(
                RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame([], columns=list(INF_SCHEMA_COLUMNS_NAMES_SET))
            )
        result.to_columns_table_response(map_type_fn=_map_type)

        return result
