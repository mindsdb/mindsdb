from typing import Any, Dict, Text

import msal
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.ast import Constant, Identifier, Select, Star
from mindsdb_sql import parse_sql
import pandas as pd
from requests.exceptions import RequestException

from mindsdb.integrations.handlers.ms_one_drive_handler.ms_graph_api_one_drive_client import MSGraphAPIOneDriveClient
from mindsdb.integrations.handlers.ms_one_drive_handler.ms_one_drive_tables import FileTable, ListFilesTable
from mindsdb.integrations.utilities.handlers.auth_utilities import MSGraphAPIDelegatedPermissionsManager
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class MSOneDriveHandler(APIHandler):
    """
    This handler handles the connection and execution of SQL statements on Microsoft OneDrive.
    """

    name = 'one_drive'
    supported_file_formats = ['csv', 'tsv', 'json', 'parquet']

    def __init__(self, name: Text, connection_data: Dict, **kwargs: Any) -> None:
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the Microsoft Graph API.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.handler_storage = kwargs['handler_storage']
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def connect(self):
        """
        Establishes a connection to Microsoft OneDrive via the Microsoft Graph API.

        Raises:
            ValueError: If the required connection parameters are not provided.
            AuthenticationError: If an error occurs during the authentication process.

        Returns:
            MSGraphAPIOneDriveClient: An instance of the Microsoft Graph API client for Microsoft OneDrive.
        """
        if self.is_connected and self.connection.check_connection():
            return self.connection

        # Mandatory connection parameters.
        if not all(key in self.connection_data for key in ['client_id', 'client_secret', 'tenant_id']):
            raise ValueError("Required parameters (client_id, client_secret, tenant_id) must be provided.")

        # Initialize the token cache.
        cache = msal.SerializableTokenCache()

        # Load the cache from file if it exists.
        cache_file = 'cache.bin'
        try:
            cache_content = self.handler_storage.file_get(cache_file)
        except FileNotFoundError:
            cache_content = None

        if cache_content:
            cache.deserialize(cache_content)

        # Initialize the Microsoft Authentication Library (MSAL) app.
        permissions_manager = MSGraphAPIDelegatedPermissionsManager(
            client_id=self.connection_data['client_id'],
            client_secret=self.connection_data['client_secret'],
            tenant_id=self.connection_data['tenant_id'],
            cache=cache,
            code=self.connection_data.get('code')
        )

        access_token = permissions_manager.get_access_token()

        # Save the cache back to file if it has changed.
        if cache.has_state_changed:
            self.handler_storage.file_set(cache_file, cache.serialize().encode('utf-8'))

        # Pass the access token to the Microsoft Graph API client for Microsoft OneDrive.
        self.connection = MSGraphAPIOneDriveClient(
            access_token=access_token,
        )

        self.is_connected = True

        return self.connection

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Microsoft Graph API for Microsoft OneDrive.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)

        try:
            connection = self.connect()
            if connection.check_connection():
                response.success = True
                response.copy_storage = True
            else:
                raise RequestException("Connection check failed!")
        except (ValueError, RequestException) as known_error:
            logger.error(f'Connection check to Microsoft OneDrive failed, {known_error}!')
            response.error_message = str(known_error)
        except Exception as unknown_error:
            logger.error(f'Connection check to Microsoft OneDrive failed due to an unknown error, {unknown_error}!')
            response.error_message = str(unknown_error)

        self.is_connected = response.success

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Executes a SQL query represented by an ASTNode and retrieves the data.

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.

        Raises:
            ValueError: If the file format is not supported.
            NotImplementedError: If the query type is not supported.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        if isinstance(query, Select):
            table_name = query.from_table.parts[-1]

            # If the table name is 'files', query the 'files' table.
            if table_name == "files":
                table = ListFilesTable(self)
                df = table.select(query)

            # For any other table name, query the file content via the 'FileTable' class.
            # Only the supported file formats can be queried.
            else:
                extension = table_name.split('.')[-1]
                if extension not in self.supported_file_formats:
                    logger.error(f'The file format {extension} is not supported!')
                    raise ValueError(f'The file format {extension} is not supported!')

                table = FileTable(self, table_name=table_name)
                df = table.select(query)

            return Response(
                RESPONSE_TYPE.TABLE,
                data_frame=df
            )

        else:
            raise NotImplementedError(
                "Only SELECT queries are supported by the Microsoft OneDrive handler."
            )

    def native_query(self, query: Text) -> Response:
        """
        Executes a SQL query and returns the result.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        query_ast = parse_sql(query)
        return self.query(query_ast)

    def get_tables(self) -> Response:
        """
        Retrieves a list of tables (files) in the user's Microsoft OneDrive.
        Each file is considered a table. Only the supported file formats are included in the list.

        Returns:
            Response: A response object containing the list of tables and views, formatted as per the `Response` class.
        """
        connection = self.connect()

        # Get only the supported file formats.
        # Wrap the file names with backticks to prevent SQL syntax errors.
        supported_files = [
            f"`{file['path']}`"
            for file in connection.get_all_items()
            if file['path'].split('.')[-1] in self.supported_file_formats
        ]

        # Add the 'files' table to the list of supported tables.
        supported_files.insert(0, 'files')

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                supported_files,
                columns=['table_name']
            )
        )

        return response

    def get_columns(self, table_name: str) -> Response:
        """
        Retrieves column details for a specified table (file) in the user's Microsoft OneDrive.

        Args:
            table_name (Text): The name of the table for which to retrieve column information.

        Returns:
            Response: A response object containing the column details, formatted as per the `Response` class.
        """
        # Get the columns (and their data types) by querying a single row from the table.
        query = Select(
            targets=[Star()],
            from_table=Identifier(parts=[table_name]),
            limit=Constant(1)
        )

        result = self.query(query)

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                {
                    'column_name': result.data_frame.columns,
                    'data_type': [data_type if data_type != 'object' else 'string' for data_type in result.data_frame.dtypes]
                }
            )
        )

        return response
