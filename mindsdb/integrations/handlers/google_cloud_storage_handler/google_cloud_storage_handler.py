import re
from typing import Text, Dict, Optional, Any

import duckdb
import pandas as pd

from google.api_core.exceptions import BadRequest
from google.cloud.storage import Client

from duckdb import DuckDBPyConnection, CatalogException
from mindsdb_sql import ASTNode, Select, Identifier

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.utilities import log
from mindsdb.integrations.utilities.handlers.auth_utilities import GoogleServiceAccountOAuth2Manager
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

logger = log.getLogger(__name__)


class GoogleCloudStorageHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the SQL statements on Google Cloud Storage.
    """

    name = 'google_cloud_storage'
    supported_file_formats = ['csv', 'tsv', 'json', 'parquet']

    def __init__(self, name: Text, connection_data: Optional[Dict], **kwargs: Any):
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the Google CLoud Storage bucket.
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
        self.thread_safe = True

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    @property
    def bucket(self) -> str:
        return self.connection_data.get('bucket')

    @property
    def prefix(self) -> str:
        return self.connection_data.get('prefix')

    @property
    def file_type(self) -> str:
        return self.connection_data.get('file_type')

    def connect(self) -> DuckDBPyConnection:
        """
        Establishes a connection to Google CLoud Storage.

        Raises:
            ValueError: If the required connection parameters are not provided or if the credentials cannot be parsed.

        Returns:
            DuckDBPyConnection: A connection object to the GCS account via DuckDB.
        """
        if self.is_connected is True:
            return self.connection

        # Validate mandatory parameters.
        if not self.bucket:
            raise ValueError('Required parameters (bucket) must be provided.')

        if not all(key in self.connection_data for key in ['gcs_access_key_id', 'gcs_secret_access_key']):
            raise ValueError(
                'Required parameters (gcs_access_key_id, gcs_secret_access_key) must be provided.')

        # Connect to GCS via DuckDB and configure mandatory credentials.
        self.connection = self._connect_duckdb()

        self.is_connected = True

        return self.connection

    def _connect_duckdb(self) -> DuckDBPyConnection:
        """
       Establishes a connection to the GCS account via DuckDB.

       Returns:
           DuckDBPyConnection: A connection object to the GCS account.
       """
        # Connect to S3 via DuckDB.
        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("INSTALL httpfs")
        duckdb_conn.execute("LOAD httpfs")

        # Configure mandatory credentials.
        duckdb_conn.execute(f"""
        CREATE SECRET(
            TYPE GCS,
            KEY_ID '{self.connection_data["gcs_access_key_id"]}',
            SECRET '{self.connection_data["gcs_secret_access_key"]}'
        );
        """)

        return duckdb_conn

    def _connect_gcs(self) -> Client:
        """
        Establishes a connection to the GCS Service account via Google Auth.

        Returns:
            google.cloud.storage.client.Client: The client object for the Google CLoud Storage connection.
        """
        # Mandatory connection parameters
        if not self.bucket:
            raise ValueError('Required parameters (bucket) must be provided.')

        google_sa_oauth2_manager = GoogleServiceAccountOAuth2Manager(
            credentials_file=self.connection_data.get('service_account_keys'),
            credentials_json=self.connection_data.get('service_account_json')
        )
        credentials = google_sa_oauth2_manager.get_oauth2_credentials()

        return Client(credentials=credentials)

    def disconnect(self):
        """
        Closes the connection to the GCS Bucket if it's currently open.
        """
        if not self.is_connected:
            return
        self.connection.close()
        self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Google Cloud Storage.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        test_file = None
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        # Check connection via Google Auth
        try:
            connection = self._connect_gcs()
            connection.list_buckets()

            # Check if the bucket exists
            connection.get_bucket(self.bucket)

            # Get the first file name to test DuckDB connection
            blobs = connection.get_bucket(self.bucket).list_blobs(prefix=self.prefix)
            for blob in blobs:
                test_file = blob.name
                break

            response.success = True
        except (BadRequest, ValueError) as e:
            logger.error(f'Error connecting to GCS Bucket {self.bucket} via Google Auth Credentials, {e}!')
            response.error_message = e

        # Check connection via DuckDB
        try:
            connection = self._connect_duckdb()
            cursor = connection.cursor()

            cursor.execute(f"SELECT * FROM 'gcs://{self.bucket}/{test_file}'")

            response.success = True

        except (BadRequest, ValueError) as e:
            logger.error(f'Error connecting to GCS Bucket {self.bucket} via DuckDB, {e}!')
            response.error_message = e

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
            # TODO: Can the creation of tables be avoided for SELECT queries?
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
                # TODO: Is it possible to avoid writing the table to a file after each query?
                self._write_table_to_file()
                response = Response(RESPONSE_TYPE.OK)

        except Exception as e:
            logger.error(f'Error running query: {query} on {self.bucket}, {e}!')
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
            connection.execute(f"CREATE TABLE IF NOT EXISTS {self.table_name} AS SELECT * FROM 'gcs://{self.bucket}/{self.key}'")
        except CatalogException as e:
            logger.error(f'Error creating table {self.table_name} from file {self.key} in {self.bucket}, {e}!')
            raise e

    def _write_table_to_file(self) -> None:
        """
        Writes the table to a file in the GCS bucket.

        Raises:
            CatalogException: If the table does not exist in the DuckDB connection.
        """
        try:
            connection = self.connect()
            connection.execute(f"COPY {self.table_name} TO 'gcs://{self.bucket}/{self.key}'")
        except CatalogException as e:
            logger.error(f'Error writing table {self.table_name} to file {self.key} in {self.bucket}, {e}!')
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
        # This will be used to create a table from the object in the S3 bucket.
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
            gcs_conn = self._connect_gcs()
            gcs_conn.get_bucket(self.bucket).get_blob(self.key)
        except Exception as e:
            logger.error(f'The file {self.key} not found in the bucket {self.bucket}, {e}!')
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
        Retrieves a list of objects in the GCS bucket.

        Each object is considered a table. Only the supported file formats are considered as tables.

        Returns:
            Response: A response object containing the list of tables and views, formatted as per the `Response` class.
        """
        client = self._connect_gcs()
        blobs = client.list_blobs(self.bucket, prefix=self.prefix)
        objects = []

        # filter blobs based on file type
        if self.file_type:
            self.supported_file_formats = [self.file_type]

        for blob in blobs:
            key = blob.name
            parts = key.split('.')

            if parts[-1] in self.supported_file_formats:
                objects.append(f"`{key}`")

        logger.info(f"Retrieved {len(objects)} objects from bucket '{self.bucket}'.")

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                objects,
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

        # TODO: Is there a more efficient way to get the column details?
        # If not, can it be limited to one column?
        query = f"SELECT * FROM {table_name} LIMIT 5"
        result = self.native_query(query)

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                {
                    'column_name': result.data_frame.columns,
                    'data_type': [data_type if data_type != 'object' else 'string' for data_type in
                                  result.data_frame.dtypes]
                }
            )
        )

        return response
