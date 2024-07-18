from typing import Optional

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import AuthenticationException, ConnectionError
from es.elastic.sqlalchemy import ESDialect
import pandas as pd
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
    RESPONSE_TYPE,
)
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class ElasticsearchHandler(DatabaseHandler):
    """
    This handler handles the connection and execution of SQL statements on Elasticsearch.
    """

    name = 'elasticsearch'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs) -> None:
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the AWS (S3) account.
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

    def connect(self) -> Elasticsearch:
        """
        Establishes a connection to the Elasticsearch host.

        Raises:
            ValueError: If the expected connection parameters are not provided.

        Returns:
            Elasticsearch: A connection object to the Elasticsearch host.
        """
        if self.is_connected is True:
            return self.connection
        
        # Mandatory connection parameters.
        if not any(key in self.connection_data for key in ['hosts', 'cloud_id']):
            raise ValueError('Either hosts or cloud_id must be provided.')
        
        config = {}

        if self.connection_data['hosts']:
            config['hosts'] = self.connection_data['hosts'].split(',')

        if self.connection_data['cloud_id']:
            config['cloud_id'] = self.connection_data['cloud_id']

        # Username and password are optional, but if one is provided, both must be provided.
        username = self.connection_data.get('username')
        password = self.connection_data.get('password')
        if username and not password:
            raise ValueError('Password must be provided along with username.')
        if password and not username:
            raise ValueError('Username must be provided along with password.')

        if username and password:
            config['basic_auth'] = (username, password)

        try:
            self.connection = Elasticsearch(
                **config,
            )
            self.is_connected = True
            return self.connection
        except ConnectionError as conn_error:
            logger.error(f'Connection error when connecting to Elasticsearch: {conn_error}')
            raise conn_error
        except AuthenticationException as auth_error:
            logger.error(f'Authentication error when connecting to Elasticsearch: {auth_error}')
            raise auth_error
        except Exception as e:
            logger.error(f'Unknown error connecting to Elasticsearch: {e}')
            raise e

    def disconnect(self) -> None:
        """
        Closes the connection to the Elasticsearch host if it's currently open.
        """
        if self.is_connected is False:
            return

        self.connection.close()
        self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Elasticsearch host.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            # TODO: Execute a simple query to check the connection.
            response.success = True
        # TODO: Make the exception handling more specific.
        except Exception as e:
            logger.error(f'Error connecting to Elasticsearch, {e}!')
            response.error_message = str(e)

        if response.success and need_to_close:
            self.disconnect()

        elif not response.success and self.is_connected:
            self.is_connected = False

        return response

    def native_query(self, query: str) -> Response:
        """
        Executes a native SQL query on the Elasticsearch host and returns the result.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        need_to_close = self.is_connected is False

        connection = self.connect()

        # TODO: Make error handling more specific.
        # TODO: Can queries outside of SELECT be executed?
        try:
            response = connection.sql.query(body={'query': query})
            records = response['rows']
            columns = response['columns']

            new_records = True
            while new_records:
                try:
                    if response['cursor']:
                        response = connection.sql.query(body={'query': query, 'cursor': response['cursor']})

                        new_records = response['rows']
                        records = records + new_records
                except Exception as e:
                    new_records = False

            if records:
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=pd.DataFrame(
                        records,
                        columns=[column['name'] for column in columns]
                    )
                )
        except Exception as e:
            logger.error(f'Error running query: {query} on Elasticsearch, {e}!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Executes a SQL query represented by an ASTNode on the Elasticsearch host and retrieves the data.

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.

        Returns:
            Response: The response from the `native_query` method, containing the result of the SQL query execution.
        """
        renderer = SqlalchemyRender(ESDialect)
        query_str = renderer.get_string(query, with_failback=True)
        logger.debug(f"Executing SQL query: {query_str}")
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        # TODO: Is 'indexes' right here?
        """
        Retrieves a list of all non-system tables (indexes) in the Elasticsearch host.

        Returns:
            Response: A response object containing a list of tables (indexes) in the Elasticsearch host.
        """
        query = """
            SHOW TABLES
        """
        result = self.native_query(query)

        df = result.data_frame
        df = df.drop(['type', 'type'], axis=1)
        result.data_frame = df.rename(columns={'name': 'table_name'})

        return result

    def get_columns(self, table_name: str) -> StatusResponse:
        """
        Retrieves column (field) details for a specified table (index) in the Elasticsearch host.

        Args:
            table_name (str): The name of the table for which to retrieve column information.

        Raises:
            ValueError: If the 'table_name' is not a valid string.

        Returns:
            Response: A response object containing the column details.
        """
        if not table_name or not isinstance(table_name, str):
            raise ValueError("Invalid table name provided.")

        query = f"""
            DESCRIBE {table_name}
        """
        result = self.native_query(query)

        df = result.data_frame
        df = df.drop('mapping', axis=1)
        result.data_frame = df.rename(columns={'column': 'column_name', 'type': 'data_type'})

        return result
