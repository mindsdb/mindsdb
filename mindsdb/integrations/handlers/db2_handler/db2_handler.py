from typing import Text, Dict, Optional, Any

import ibm_db_dbi
from ibm_db_dbi import OperationalError, ProgrammingError
from ibm_db_sa.ibm_db import DB2Dialect_ibm_db as DB2Dialect
from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
import pandas as pd

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class DB2Handler(DatabaseHandler):
    name = "db2"

    def __init__(self, name: Text, connection_data: Optional[Dict], **kwargs: Any) -> None:
        """
        Initializes the handler.
        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the IBM Db2 database.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def __del__(self) -> None:
        """
        Closes the connection when the handler instance is deleted.
        """
        if self.is_connected:
            self.disconnect()

    def connect(self) -> ibm_db_dbi.Connection:
        """
        Establishes a connection to a IBM Db2 database.

        Raises:
            ValueError: If the required connection parameters are not provided.
            ibm_db_dbi.OperationalError: If an error occurs while connecting to the IBM Db2 database.

        Returns:
            ibm_db_dbi.Connection: A connection object to the IBM Db2 database.
        """
        if self.is_connected:
            return self.connection

        # Mandatory connection parameters.
        if not all(key in self.connection_data for key in ["host", "user", "password", "database"]):
            raise ValueError("Required parameters (host, user, password, database) must be provided.")
        cloud = "databases.appdomain.cloud" in self.connection_data["host"]
        if cloud:
            connection_string = f"DATABASE={self.connection_data['database']};HOSTNAME={self.connection_data['host']};PORT={self.connection_data['port']};PROTOCOL=TCPIP;UID={self.connection_data['user']};PWD={self.connection_data['password']};SECURITY=SSL;"
            connection_string += "SSLSERVERCERTIFICATE=;"
        else:
            connection_string = f"DRIVER={'IBM DB2 ODBC DRIVER'};DATABASE={self.connection_data['database']};HOST={self.connection_data['host']};PROTOCOL=TCPIP;UID={self.connection_data['user']};PWD={self.connection_data['password']};"

            # Optional connection parameters.
            if "port" in self.connection_data:
                connection_string += f"PORT={self.connection_data['port']};"

        if "schema" in self.connection_data:
            connection_string += f"CURRENTSCHEMA={self.connection_data['schema']};"

        try:
            self.connection = ibm_db_dbi.pconnect(connection_string, "", "")
            self.is_connected = True
            return self.connection
        except OperationalError as operational_error:
            logger.error(f"Error while connecting to {self.connection_data.get('database')}, {operational_error}!")
            raise
        except Exception as unknown_error:
            logger.error(f"Unknown error while connecting to {self.connection_data.get('database')}, {unknown_error}!")
            raise

    def disconnect(self) -> None:
        """
        Closes the connection to the IBM Db2 database if it's currently open.
        """
        if not self.is_connected:
            return

        self.connection.close()
        self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the IBM Db2 database.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            response.success = True
        except (OperationalError, ValueError) as known_error:
            logger.error(f"Connection check to IBM Db2 failed, {known_error}!")
            response.error_message = str(known_error)
        except Exception as unknown_error:
            logger.error(f"Connection check to IBM Db2 failed due to an unknown error, {unknown_error}!")
            response.error_message = str(unknown_error)

        if response.success and need_to_close:
            self.disconnect()

        elif not response.success and self.is_connected:
            self.is_connected = False

        return response

    def native_query(self, query: Text) -> Response:
        """
        Executes a SQL query on the IBM Db2 database and returns the result (if any).

        Args:
            query (str): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        need_to_close = self.is_connected is False

        connection = self.connect()
        with connection.cursor() as cur:
            try:
                cur.execute(query)

                if cur._result_set_produced:
                    result = cur.fetchall()
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        data_frame=pd.DataFrame(result, columns=[x[0] for x in cur.description]),
                    )
                else:
                    response = Response(RESPONSE_TYPE.OK)
                connection.commit()
            except (OperationalError, ProgrammingError) as known_error:
                logger.error(f"Error running query: {query} on {self.connection_data.get('database')}!")
                response = Response(RESPONSE_TYPE.ERROR, error_message=str(known_error))
                connection.rollback()

            except Exception as unknown_error:
                logger.error(f"Unknown error running query: {query} on {self.connection_data.get('database')}!")
                response = Response(RESPONSE_TYPE.ERROR, error_message=str(unknown_error))
                connection.rollback()

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Executes a SQL query represented by an ASTNode on the IBM Db2 database and retrieves the data (if any).

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.

        Returns:
            Response: The response from the `native_query` method, containing the result of the SQL query execution.
        """
        renderer = SqlalchemyRender(DB2Dialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Retrieves a list of all non-system tables and views in the current schema of the IBM Db2 database.

        Returns:
            Response: A response object containing the list of tables and views, formatted as per the `Response` class.
        """
        connection = self.connect()

        result = connection.tables(connection.current_schema)

        tables = []
        for table in result:
            tables.append(
                {
                    "TABLE_NAME": table["TABLE_NAME"],
                    "TABLE_SCHEMA": table["TABLE_SCHEM"],
                    "TABLE_TYPE": table["TABLE_TYPE"],
                }
            )

        response = Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame(tables))

        return response

    def get_columns(self, table_name: Text) -> Response:
        """
        Retrieves column details for a specified table in the IBM Db2 database.

        Args:
            table_name (Text): The name of the table for which to retrieve column information.

        Raises:
            ValueError: If the 'table_name' is not a valid string.

        Returns:
            Response: A response object containing the column details.
        """
        if not table_name or not isinstance(table_name, str):
            raise ValueError("Invalid table name provided.")

        connection = self.connect()

        result = connection.columns(table_name=table_name)

        columns = [column["COLUMN_NAME"] for column in result]

        response = Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame(columns, columns=["COLUMN_NAME"]))

        return response
