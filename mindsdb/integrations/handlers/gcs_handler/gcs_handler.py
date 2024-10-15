import re
import duckdb
import pandas as pd
import fsspec
import google.auth
from google.cloud import storage

from typing import Text, Dict, Optional
from duckdb import DuckDBPyConnection, CatalogException

from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.ast import Select, Identifier

from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

from mindsdb.integrations.libs.base import DatabaseHandler


logger = log.getLogger(__name__)


class GcsHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the SQL statements on GCS.
    """

    name = 'gcs'
    
    supported_file_formats = ['csv', 'tsv', 'json', 'parquet']

    def __init__(self, name: Text, connection_data: Optional[Dict], **kwargs):
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the GCS account.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs
        self.is_select_query = False
        self.key = None
        self.table_name = None

        self.connection = None
        self.is_connected = False
    
    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> DuckDBPyConnection:
        """
        Establishes a connection to the GCS account via DuckDB.

        Raises:
            KeyError: If the required connection parameters are not provided.

        Returns:
            DuckDBPyConnection : A client object to the GCS account.
        """
        if self.is_connected is True:
            return self.connection

        # Validate mandatory parameters.
        if not all(key in self.connection_data for key in ['service_account_json_file_path', 'bucket']):
            raise ValueError('Required parameters (service_account_json_file_path, bucket) must be provided.')

        # Connect to GCS via DuckDB and configure mandatory credentials.
        self.connection = self._connect_duckdb()

        self.is_connected = True

        return self.connection

    def _connect_duckdb(self) -> DuckDBPyConnection:
        """
        Establishes a connection to the GCP account via DuckDB.

        Returns:
            DuckDBPyConnection: A connection object to the GCP account.
        """
        # Connect to GCS via DuckDB.
        credentials, project_id = google.auth.load_credentials_from_file(self.connection_data["service_account_json_file_path"])
        gcs = fsspec.filesystem("gcs", project=project_id, credentials=credentials)
        duckdb_conn = duckdb.connect()
        duckdb_conn.register_filesystem(gcs)

        return duckdb_conn

    def _connect_storage_client(self) -> storage.Client:
        """
        Establishes a connection to the GCS account via google-cloud-storage.

        Returns:
            storage.Client: A client object to the GCS account.
        """
        return storage.Client.from_service_account_json(self.connection_data["service_account_json_file_path"])

    
    def disconnect(self):
        """
        Closes the connection to the GCS account if it's currently open.
        """
        if not self.is_connected:
            return
        self.connection.close()
        self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the GCS bucket.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        # Check connection via storage client.
        try:
            storage_client = self._connect_storage_client()
            storage_client.get_bucket(self.connection_data['bucket'])
            response.success = True
            storage_client.close()
        except Exception as e:
            logger.error(f'Error connecting to GCS with the given credentials, {e}!')
            response.error_message = str(e)

        if response.success and need_to_close:
            self.disconnect()

        elif not response.success and self.is_connected:
            self.is_connected = False

        return response

    def native_query(self, query: Text) -> Response:
        """
        Executes a SQL query on the specified table (object) in the GCS bucket.

        Args:
            query (Text): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        need_to_close = not self.is_connected

        connection = self.connect()
        cursor = connection.cursor()

        try:
            self._create_table_if_not_exists_from_file()

            cursor.execute(query)
            if self.is_select_query:
                result = cursor.fetchall()
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=pd.DataFrame(
                        result,
                        columns=[x[0] for x in cursor.description]
                    )
                )

            else:
                connection.commit()
                self._write_table_to_file()
                response = Response(RESPONSE_TYPE.OK)
        except Exception as e:
            logger.error(f'Error running query: {query} on {self.connection_data["bucket"]}, {e}!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

        if need_to_close is True:
            self.disconnect()

        return response

    def _create_table_if_not_exists_from_file(self) -> None:
        """
        Creates a table from a file in the GCS bucket.

        Raises:
            CatalogException: If the file does not exist in the GCS bucket.
        """
        connection = self.connect()
        try:
            connection.execute(f"CREATE TABLE IF NOT EXISTS {self.table_name} AS SELECT * FROM 'gs://{self.connection_data['bucket']}/{self.key}'")
        except CatalogException as e:
            logger.error(f'Error creating table {self.table_name} from file {self.key} in {self.connection_data["bucket"]}, {e}!')
            raise e

    def _write_table_to_file(self) -> None:
        """
        Writes the table to a file in the GCS bucket.

        Raises:
            CatalogException: If the table does not exist in the DuckDB connection.
        """
        try:
            connection = self.connect()
            connection.execute(f"COPY {self.table_name} TO 'gs://{self.connection_data['bucket']}/{self.key}'")
        except CatalogException as e:
            logger.error(f'Error writing table {self.table_name} to file {self.key} in {self.connection_data["bucket"]}, {e}!')
            raise e

    def query(self, query: ASTNode) -> Response:
        """
        Executes a SQL query represented by an ASTNode and retrieves the data.

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.

        Raises:
            ValueError: If the file format is not supported or the file does not exist in the GCS bucket.

        Returns:
            Response: The response from the `native_query` method, containing the result of the SQL query execution.
        """
        # Set the key by getting it from the query.
        # This will be used to create a table from the object in the GCS bucket.

        if isinstance(query, Select):
            self.is_select_query = True
            table = query.from_table
        else:
            table = query.table
            

        self.key = table.get_string().replace('`', '')

        # Check if the file format is supported.
        if self.key.split('.')[-1] not in self.supported_file_formats:
            logger.error(f'The file format {self.key.split(".")[-1]} is not supported!')
            raise ValueError(f'The file format {self.key.split(".")[-1]} is not supported!')

        # Check if the file exists in the GCS bucket.
        try:
            storage_client = self._connect_storage_client()
            bucket = storage_client.bucket(self.connection_data['bucket'])
            stats = storage.Blob(bucket=bucket, name=self.key).exists(storage_client)
            storage_client.close()
            if not stats:
                raise Exception(f'{self.key} not found in the bucket {self.connection_data["bucket"]}')
        except Exception as e:
            logger.error(f'Error querying the file {self.key} in the bucket {self.connection_data["bucket"]}, {e}!')
            raise e

        # Replace all special characters in the key with underscores to create a valid table name.
        self.table_name = re.sub(r'[\W]+', '_', self.key)

        # Replace the key with the name of the table to be created.
        if self.is_select_query:
            query.from_table = Identifier(
                parts=[self.table_name],
                alias=table.alias
            )

        else:
            query.table = Identifier(
                parts=[self.table_name],
                alias=table.alias
            )

        return self.native_query(query.to_string())


    def get_tables(self) -> Response:
        """
        Retrieves a list of tables (objects) in the GCS bucket.

        Each object is considered a table. Only the supported file formats are considered as tables.

        Returns:
            Response: A response object containing the list of tables and views, formatted as per the `Response` class.
        """
        storage_client = self._connect_storage_client()
        objects = storage_client.list_blobs(self.connection_data["bucket"])
        storage_client.close()
        # Get only the supported file formats.
        # Sorround the object names with backticks to prevent SQL syntax errors.
        supported_objects = [f"`{obj.name}`" for obj in objects if obj.name.split('.')[-1] in self.supported_file_formats]

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                supported_objects,
                columns=['table_name']
            )
        )

        return response

    def get_columns(self, table_name: Text) -> Response:
        """
        Retrieves column details for a specified table (object) in the GCS bucket.

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
        result = self.native_query(query)

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
    