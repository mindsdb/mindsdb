from contextlib import contextmanager

import json
import duckdb
import pandas as pd
import fsspec
import google.auth
from google.cloud import storage
from typing import Text, Dict, Optional, List
from duckdb import DuckDBPyConnection

from mindsdb.integrations.handlers.gcs_handler.gcs_tables import (
    ListFilesTable,
    FileTable
)
from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb_sql_parser.ast import Select, Identifier, Insert, Star, Constant

from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

from mindsdb.integrations.libs.api_handler import APIHandler

logger = log.getLogger(__name__)


class GcsHandler(APIHandler):
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
        self.service_account_json = None
        self.connection = None

        if 'service_account_keys' not in self.connection_data and 'service_account_json' not in self.connection_data:
            raise ValueError('service_account_keys or service_account_json parameter must be provided.')

        if 'service_account_json' in self.connection_data:
            self.service_account_json = self.connection_data["service_account_json"]

        if 'service_account_keys' in self.connection_data:
            with open(self.connection_data["service_account_keys"], "r") as f:
                self.service_account_json = json.loads(f.read())

        self.is_connected = False

        self._files_table = ListFilesTable(self)

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

        # Connect to GCS and configure mandatory credentials.
        self.connection = self._connect_storage_client()
        self.is_connected = True

        return self.connection

    @contextmanager
    def _connect_duckdb(self):
        """
        Creates temporal duckdb database which is able to connect to the GCS account.
        Have to be used as context manager

        Returns:
            DuckDBPyConnection
        """
        # Connect to GCS via DuckDB.
        duckdb_conn = duckdb.connect(":memory:")

        # Configure mandatory credentials.
        credentials, project_id = google.auth.load_credentials_from_dict(self.service_account_json)
        gcs = fsspec.filesystem("gcs", project=project_id, credentials=credentials)
        duckdb_conn = duckdb.connect()
        duckdb_conn.register_filesystem(gcs)

        try:
            yield duckdb_conn
        finally:
            duckdb_conn.close()

    def _connect_storage_client(self) -> storage.Client:
        """
        Establishes a connection to the GCS account via google-cloud-storage.

        Returns:
            storage.Client: A client object to the GCS account.
        """
        return storage.Client.from_service_account_info(self.service_account_json)

    def disconnect(self):
        """
        Closes the connection to the GCP account if it's currently open.
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
            if 'bucket' in self.connection_data:
                storage_client.get_bucket(self.connection_data['bucket'])
            else:
                storage_client.list_buckets()
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

    def _get_bucket(self, key):
        if 'bucket' in self.connection_data:
            return self.connection_data['bucket'], key

        # get bucket from first part of the key
        ar = key.split('/')
        return ar[0], '/'.join(ar[1:])

    def read_as_table(self, key) -> pd.DataFrame:
        """
        Read object as dataframe. Uses duckdb
        """

        bucket, key = self._get_bucket(key)

        with self._connect_duckdb() as connection:

            cursor = connection.execute(f"SELECT * FROM 'gs://{bucket}/{key}'")

            return cursor.fetchdf()

    def _read_as_content(self, key) -> None:
        """
        Read object as content
        """
        bucket, key = self._get_bucket(key)

        client = self.connect()

        bucket = client.bucket(bucket)
        blob = bucket.blob(key)
        return blob.download_as_string()

    def add_data_to_table(self, key, df) -> None:
        """
        Writes the table to a file in the gcs bucket.

        Raises:
            CatalogException: If the table does not exist in the DuckDB connection.
        """

        # Check if the file exists in the gcs bucket.
        bucket, key = self._get_bucket(key)

        storage_client = self._connect_storage_client()
        bucketObj = storage_client.bucket(bucket)
        stats = storage.Blob(bucket=bucketObj, name=key).exists(storage_client)
        storage_client.close()
        if not stats:
            raise Exception(f'Error querying the file {key} in the bucket {bucket}!')

        with self._connect_duckdb() as connection:
            # copy
            connection.execute(f"CREATE TABLE tmp_table AS SELECT * FROM 'gs://{bucket}/{key}'")

            # insert
            connection.execute("INSERT INTO tmp_table BY NAME SELECT * FROM df")

            # upload
            connection.execute(f"COPY tmp_table TO 'gs://{bucket}/{key}'")

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

        self.connect()

        if isinstance(query, Select):
            table_name = query.from_table.parts[-1]

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

    def get_objects(self, limit=None, buckets=None) -> List[dict]:
        storage_client = self._connect_storage_client()
        if "bucket" in self.connection_data:
            add_bucket_to_name = False
            scan_buckets = [self.connection_data["bucket"]]
        else:
            add_bucket_to_name = True
            scan_buckets = [b.name for b in storage_client.list_buckets()]

        objects = []
        for bucket in scan_buckets:
            if buckets is not None and bucket not in buckets:
                continue

            blobs = storage_client.list_blobs(bucket)
            if not blobs:
                continue

            for blob in blobs:
                if blob.storage_class != 'STANDARD':
                    continue

                obj = {}
                obj['Bucket'] = bucket
                if add_bucket_to_name:
                    # bucket is part of the name
                    obj['Key'] = f'{bucket}/{blob.name}'
                objects.append(obj)
            if limit is not None and len(objects) >= limit:
                break

        return objects

    def get_tables(self) -> Response:
        """
        Retrieves a list of tables (objects) in the gcs bucket.

        Each object is considered a table. Only the supported file formats are considered as tables.

        Returns:
            Response: A response object containing the list of tables and views, formatted as per the `Response` class.
        """

        # Get only the supported file formats.
        # Wrap the object names with backticks to prevent SQL syntax errors.
        supported_names = [
            f"`{obj['Key']}`"
            for obj in self.get_objects()
            if obj['Key'].split('.')[-1] in self.supported_file_formats
        ]

        # virtual table with list of files
        supported_names.insert(0, 'files')

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                supported_names,
                columns=['table_name']
            )
        )

        return response

    def get_columns(self, table_name: str) -> Response:
        """
        Retrieves column details for a specified table (object) in the gcs bucket.

        Args:
            table_name (Text): The name of the table for which to retrieve column information.

        Raises:
            ValueError: If the 'table_name' is not a valid string.

        Returns:
            Response: A response object containing the column details, formatted as per the `Response` class.
        """
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
