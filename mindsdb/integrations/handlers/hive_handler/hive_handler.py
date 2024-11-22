from typing import Text, Dict, Optional

from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql_parser.ast.base import ASTNode
import pandas as pd
from pyhive import (hive, sqlalchemy_hive)
from pyhive.exc import OperationalError
from thrift.transport.TTransport import TTransportException

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class HiveHandler(DatabaseHandler):
    """
    This handler handles the connection and execution of SQL statements on Apache Hive.
    """

    name = 'hive'

    def __init__(self, name: Text, connection_data: Optional[Dict], **kwargs) -> None:
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the Apache Hive server.
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

    def connect(self) -> hive.Connection:
        """
        Establishes a connection to the Apache Hive server.

        Raises:
            ValueError: If the expected connection parameters are not provided.

        Returns:
            hive.Connection: A connection object to the Apache Hive server.
        """
        if self.is_connected:
            return self.connection

        # Mandatory connection parameters.
        if not all(key in self.connection_data for key in ['host', 'database']):
            raise ValueError('Required parameters (account, database) must be provided.')

        config = {
            'host': self.connection_data.get('host'),
            'database': self.connection_data.get('database')
        }

        # Optional connection parameters.
        optional_parameters = ['port', 'username', 'password']
        for param in optional_parameters:
            if param in self.connection_data:
                config[param] = self.connection_data[param]

        config['auth'] = self.connection_data.get('auth', 'CUSTOM').upper()

        try:
            self.connection = hive.Connection(**config)
            self.is_connected = True
            return self.connection
        except (OperationalError, TTransportException, ValueError) as known_error:
            logger.error(f'Error connecting to Hive {config["database"]}, {known_error}!')
            raise
        except Exception as unknown_error:
            logger.error(f'Unknown error connecting to Hive {config["database"]}, {unknown_error}!')
            raise

    def disconnect(self) -> None:
        """
        Closes the connection to the Apache Hive server if it's currently open.
        """
        if self.is_connected is False:
            return

        self.connection.close()
        self.is_connected = False
        return

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Apache Hive server.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            response.success = True
        except (OperationalError, TTransportException, ValueError) as known_error:
            logger.error(f'Connection check to Hive failed, {known_error}!')
            response.error_message = str(known_error)
        except Exception as unknown_error:
            logger.error(f'Connection check to Hive failed due to an unknown error, {unknown_error}!')
            response.error_message = str(unknown_error)

        if response.success is True and need_to_close:
            self.disconnect()
        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query: Text) -> Response:
        """
        Executes a native SQL query on the Apache Hive server and returns the result.

        Args:
            query (Text): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        need_to_close = self.is_connected is False

        connection = self.connect()
        with connection.cursor() as cur:
            try:
                cur.execute(query)
                result = cur.fetchall()
                if result:
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        pd.DataFrame(
                            result,
                            columns=[x[0].split('.')[-1] for x in cur.description]
                        )
                    )
                else:
                    response = Response(RESPONSE_TYPE.OK)
                connection.commit()
            except OperationalError as operational_error:
                logger.error(f'Error running query: {query} on {self.connection_data["database"]}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(operational_error)
                )
                connection.rollback()
            except Exception as unknown_error:
                logger.error(f'Unknown error running query: {query} on {self.connection_data["database"]}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(unknown_error)
                )
                connection.rollback()

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Executes a SQL query represented by an ASTNode on the Apache Hive server and retrieves the data (if any).

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.

        Returns:
            Response: The response from the `native_query` method, containing the result of the SQL query execution.
        """
        renderer = SqlalchemyRender(sqlalchemy_hive.HiveDialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Retrieves a list of all non-system tables in the Apache Hive server.

        Returns:
            Response: A response object containing a list of tables in the Apache Hive server.
        """
        q = "SHOW TABLES"
        result = self.native_query(q)
        df = result.data_frame
        result.data_frame = df.rename(columns={df.columns[0]: 'table_name'})
        return result

    def get_columns(self, table_name: Text) -> Response:
        """
        Retrieves column details for a specified table in the Apache Hive server.

        Args:
            table_name (Text): The name of the table for which to retrieve column information.

        Raises:
            ValueError: If the 'table_name' is not a valid string.

        Returns:
            Response: A response object containing the column details.
        """
        if not table_name or not isinstance(table_name, str):
            raise ValueError("Invalid table name provided.")

        q = f"DESCRIBE {table_name}"
        result = self.native_query(q)
        return result
