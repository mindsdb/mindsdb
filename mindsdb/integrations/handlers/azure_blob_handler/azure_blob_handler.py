import pandas as pd
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.utilities import log
import duckdb
from duckdb import DuckDBPyConnection
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, generate_account_sas, ResourceTypes, AccountSasPermissions
from datetime import datetime, timedelta
from contextlib import contextmanager
from typing import List,Text, Dict
from mindsdb.integrations.libs.api_handler import APIResource, APIHandler
from mindsdb.integrations.utilities.sql_utils import FilterCondition
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.ast import Select, Identifier, Insert, Star, Constant


logger = log.getLogger(__name__)

class ListFilesTable(APIResource):

    def list(self,
             targets: List[str] = None,
             conditions: List[FilterCondition] = None,
             *args, **kwargs) -> pd.DataFrame:

        tables = self.handler._get_tables()
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
        return self.handler._read_as_table(table_name)

    def add(self, data, table_name=None):
        df = pd.DataFrame(data)
        return self.handler._add_data_to_table(table_name, df)



class AzureBlobHandler(APIHandler):
    """
    This handler handles connection and execution of the SQL statements on Azure Blob.
    """

    name = 'azureblob'
    supported_file_formats = ['csv', 'tsv', 'json', 'parquet']

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        """ constructor
        Args:
            name (str): the handler name
        """

        self.connection = None
        self.is_connected = False

        self._tables = {}
        self.storage_account_name = None
        self.account_access_key = None
        
        self._files_table = ListFilesTable(self)
        self.container_name = None

        connection_data = kwargs.get('connection_data')
        if 'storage_account_name' in connection_data:
            self.storage_account_name = connection_data['storage_account_name']

        if 'account_access_key' in connection_data:
            self.account_access_key = connection_data['account_access_key']

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

        sas_token = generate_account_sas(
            account_name=self.storage_account_name,
            account_key=self.account_access_key,
            resource_types=ResourceTypes(service=True,container=True,object=True),
            permission=AccountSasPermissions(read=True,list=True),
            expiry=datetime.now() + timedelta(hours=1)
        )

        blob_service_client = BlobServiceClient(
            account_url=f"https://{self.storage_account_name}.blob.core.windows.net",
            credential=sas_token
        )
        self.connection = blob_service_client
        self.is_connected = True
        return blob_service_client

    def check_connection(self) -> StatusResponse:
        """ Check connection to the handler
        Returns:
            HandlerStatusResponse
        """
        response = StatusResponse(False)

        try:
            client = self.connect()
            client.get_account_information()
            response.success = True

        except Exception as e:
            logger.error(f'Error connecting to Azure Blob: {e}!')
            response.error_message = e

        self.is_connected = response.success
        return response

    def native_query(self, query: str = None) -> Response:
        """Receive raw query and act upon it somehow.
        Args:
            query (Any): query in native format (str for sql databases,
                dict for mongo, api's json etc)
        Returns:
            HandlerResponse
        """

    def call_application_api(self, method_name:str = None, params:dict = None) -> pd.DataFrame:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. Can be any kind
                of query: SELECT, INSERT, DELETE, etc
        Returns:
            DataFrame
        """

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
        duckdb_conn.execute('''INSTALL azure''')
        duckdb_conn.execute('''LOAD azure''')

        # Configure mandatory credentials.
        duckdb_conn.execute(f'SET azure_storage_connection_string="{self.connection_string}"')

        try:
            yield duckdb_conn
        finally:
            duckdb_conn.close()

    def _read_as_table(self, key) -> pd.DataFrame:
        """
        Read object as dataframe. Uses duckdb
        """

        with self._connect_duckdb() as connection:
            # connection.execute('SET azure_transport_option_type="curl"')
            cursor = connection.execute(f'SELECT * FROM "azure://{self.container_name}/{key}"')

            return cursor.fetchdf()

    def _read_as_content(self, key) -> None:
        """
        Read object as content
        """

        coonection = self.connect()
        client = coonection.get_blob_client(container=self.container_name,blob=key)
        # bucket = client.bucket(self.connection_data['bucket'])
        # blob = bucket.blob(key)
        return client.download_blob()

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

    def get_tables(self) -> Response:
        """
        Retrieves a list of tables (objects) in the Azure Containers.

        Each object is considered a table. Only the supported file formats are considered as tables.

        Returns:
            Response: A response object containing the list of tables and views, formatted as per the `Response` class.
        """

        # Get only the supported file formats.
        # Wrap the object names with backticks to prevent SQL syntax errors.

        client = self.connect()

        container_client = client.get_container_client(self.container_name)

        all_files = container_client.list_blobs()

        # Wrap the object names with backticks to prevent SQL syntax errors.
        supported_files = [f"`{file.get('name')}`" for file in all_files if file.get('name').split('.')[-1] in self.supported_file_formats]

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
        # result = self.native_query(query)

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