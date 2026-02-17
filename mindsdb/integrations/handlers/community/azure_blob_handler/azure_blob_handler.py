from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.utilities import log
import duckdb
import pandas as pd

from azure.storage.blob import BlobServiceClient

from contextlib import contextmanager
from typing import List, Text, Optional, Dict
from mindsdb.integrations.libs.api_handler import APIResource, APIHandler
from mindsdb.integrations.utilities.sql_utils import FilterCondition
from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb_sql_parser.ast import Select, Identifier, Insert
from mindsdb_sql_parser import parse_sql

logger = log.getLogger(__name__)


class ListFilesTable(APIResource):

    def list(self,
             targets: List[str] = None,
             conditions: List[FilterCondition] = None,
             *args, **kwargs) -> pd.DataFrame:

        tables = self.handler.get_files()
        data = []
        for path in tables:
            path = path.replace('`', '')
            item = {
                'path': path,
                'name': path[path.rfind('/') + 1:],
                'extension': path[path.rfind('.') + 1:]
            }

            data.append(item)

        return pd.DataFrame(data=data, columns=self.get_columns())

    def get_columns(self) -> List[str]:
        return ["path", "name", "extension", "content"]


class FileTable(APIResource):

    def list(self, targets: List[str] = None, table_name=None, *args, **kwargs) -> pd.DataFrame:
        return self.handler.read_as_table(table_name)

    def add(self, data, table_name=None):
        df = pd.DataFrame(data)
        return self.handler.add_data_to_table(table_name, df)


class AzureBlobHandler(APIHandler):
    """
    This handler handles connection and execution of the SQL statements on Azure Blob.
    """

    name = "azureblob"
    supported_file_formats = ['csv', 'tsv', 'json', 'parquet']

    def __init__(self, name: Text, connection_data: Optional[Dict], **kwargs):
        super().__init__(name)
        """ constructor
        Args:
            name (str): the handler name
        """

        self.connection = None
        self.is_connected = False
        self._tables = {}
        self._files_table = ListFilesTable(self)
        self.container_name = None

        self.connection_data = connection_data

        if 'container_name' in connection_data:
            self.container_name = connection_data['container_name']

        if 'connection_string' in connection_data:
            self.connection_string = connection_data['connection_string']

    def connect(self) -> BlobServiceClient:
        """ Set up any connections required by the handler
        Should return output of check_connection() method after attempting
        connection. Should switch self.is_connected.
        Returns:
            HandlerStatusResponse
        """
        if self.is_connected is True:
            return self.connection

        blob_service_client = BlobServiceClient.from_connection_string(conn_str=self.connection_string)

        self.connection = blob_service_client
        self.is_connected = True
        return blob_service_client

    def check_connection(self) -> StatusResponse:
        """ Check connection to the handler
        Returns:
            HandlerStatusResponse
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            client = self.connect()
            client.get_account_information()
            response.success = True

        except Exception as e:
            logger.error(f'Error connecting to Azure Blob: {e}!')
            response.error_message = e

        if response.success and need_to_close:
            self.disconnect()

        elif not response.success and self.is_connected:
            self.is_connected = False

        return response

    def disconnect(self):
        """
        Closes the connection to the Azure Blob account if it's currently open.
        """
        if not self.is_connected:
            return
        self.connection.close()
        self.is_connected = False

    @contextmanager
    def _connect_duckdb(self):
        """
        Creates temporal duckdb database which is able to connect to the Azure Blob account.
        Have to be used as context manager

        Returns:
            DuckDBPyConnection
        """
        # Connect to Azure Blob via DuckDB.
        duckdb_conn = duckdb.connect(":memory:")
        duckdb_conn.execute('INSTALL azure')
        duckdb_conn.execute('LOAD azure')

        # Configure mandatory credentials.
        duckdb_conn.execute(f'SET azure_storage_connection_string="{self.connection_string}"')

        try:
            yield duckdb_conn
        finally:
            duckdb_conn.close()

    def read_as_table(self, key) -> pd.DataFrame:
        """
        Read object as dataframe. Uses duckdb
        """

        with self._connect_duckdb() as connection:
            cursor = connection.execute(f'SELECT * FROM "azure://{self.container_name}/{key}"')
            return cursor.fetchdf()

    def _read_as_content(self, key) -> None:
        """
        Read object as content
        """

        connection = self.connect()
        client = connection.get_blob_client(container=self.container_name, blob=key)

        return client.download_blob()

    def add_data_to_table(self, key, df) -> None:
        pass
        """
        Writes the table to a file in the azure container.

        Raises:
            CatalogException: If the table does not exist in the DuckDB connection.
        """

        # Check if the file exists in the Container.

        try:
            client = self.connect()
            blob_client = client.get_blob_client(container=self.container_name, blob=key)
            blob_client.close()

        except Exception as e:
            logger.error(f'Error querying the file {key} in the container {self.container_name}, {e}!')
            raise e

        with self._connect_duckdb() as connection:
            # copy
            connection.execute(f'CREATE TABLE tmp_table AS SELECT * FROM "azure://{self.container_name}/{key}"')

            # insert
            connection.execute("INSERT INTO tmp_table BY NAME SELECT * FROM df")

            # upload
            connection.execute(f"COPY tmp_table TO 'azure://{self.container_name}/{key}'")

    def native_query(self, query: str) -> Response:
        """
        Executes a SQL query and returns the result.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        query_ast = parse_sql(query)
        return self.query(query_ast)

    def query(self, query: ASTNode) -> Response:
        """
        Executes a SQL query represented by an ASTNode and retrieves the data.

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.

        Raises:
            ValueError: If the file format is not supported or the file does not exist in the Azure Blob Container.

        Returns:
            Response: The response from the `native_query` method, containing the result of the SQL query execution.
        """

        self.connect()

        if isinstance(query, Select):
            table_name = query.from_table.parts[-1].replace('`', '')

            if table_name == 'files':
                table = self._files_table
                df = table.select(query)

                # add content
                has_content = False
                for target in query.targets:
                    if isinstance(target, Identifier) and target.parts[-1].lower() == 'content':
                        has_content = True
                        break
                if has_content:
                    df['content'] = df['path'].apply(self._read_as_content)
            else:
                extension = table_name.split('.')[-1]
                if extension not in self.supported_file_formats:
                    logger.error(f'The file format {extension} is not supported!')
                    raise ValueError(f'The file format {extension} is not supported!')

                table = FileTable(self, table_name=table_name)
                df = table.select(query)

            response = Response(
                RESPONSE_TYPE.TABLE,
                data_frame=df
            )
        elif isinstance(query, Insert):
            table_name = query.table.parts[-1]
            table = FileTable(self, table_name=table_name)
            table.insert(query)
            response = Response(RESPONSE_TYPE.OK)
        else:
            raise NotImplementedError

        return response

    def get_files(self) -> List[str]:
        client = self.connect()
        container_client = client.get_container_client(self.container_name)
        all_files = container_client.list_blobs()

        # Wrap the object names with backticks to prevent SQL syntax errors.
        supported_files = [
            f"`{file.get('name')}`"
            for file in all_files if file.get('name').split('.')[-1] in self.supported_file_formats
        ]

        return supported_files

    def get_tables(self) -> Response:
        """
        Retrieves a list of tables (objects) in the Azure Containers.

        Each object is considered a table. Only the supported file formats are considered as tables.

        Returns:
            Response: A response object containing the list of tables and views, formatted as per the `Response` class.
        """

        # Get only the supported file formats.
        # Wrap the object names with backticks to prevent SQL syntax errors.

        supported_files = self.get_files()

        # virtual table with list of files
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
        Retrieves column details for a specified table (object) in the Azure Blob Container.

        Args:
            table_name (Text): The name of the table for which to retrieve column information.

        Raises:
            ValueError: If the 'table_name' is not a valid string.

        Returns:
            Response: A response object containing the column details, formatted as per the `Response` class.
        """
        if not table_name or not isinstance(table_name, str):
            raise ValueError("Invalid table name provided.")

        query = f"SELECT * FROM {table_name} LIMIT 5"

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
