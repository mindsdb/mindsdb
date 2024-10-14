from typing import List
from contextlib import contextmanager

import boto3
import duckdb
import pandas as pd
from typing import Text, Dict, Optional
from botocore.exceptions import ClientError
from duckdb import DuckDBPyConnection

from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.ast import Select, Identifier, Insert

from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

from mindsdb.integrations.libs.api_handler import APIResource, APIHandler
from mindsdb.integrations.utilities.sql_utils import FilterCondition

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


class S3Handler(APIHandler):
    """
    This handler handles connection and execution of the SQL statements on AWS S3.
    """

    name = 's3'
    # TODO: Can other file formats be supported?
    supported_file_formats = ['csv', 'tsv', 'json', 'parquet']

    def __init__(self, name: Text, connection_data: Optional[Dict], **kwargs):
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
        self.thread_safe = True

        self._files_table = ListFilesTable(self)

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> DuckDBPyConnection:
        """
        Establishes a connection to the AWS (S3) account.

        Raises:
            ValueError: If the required connection parameters are not provided.

        Returns:
            boto3.client: A client object to the AWS (S3) account.
        """
        if self.is_connected is True:
            return self.connection

        # Validate mandatory parameters.
        if not all(key in self.connection_data for key in ['aws_access_key_id', 'aws_secret_access_key', 'bucket']):
            raise ValueError('Required parameters (aws_access_key_id, aws_secret_access_key, bucket) must be provided.')

        # Connect to S3 and configure mandatory credentials.
        self.connection = self._connect_boto3()
        self.is_connected = True

        return self.connection

    @contextmanager
    def _connect_duckdb(self) -> DuckDBPyConnection:
        """
        Creates temporal duckdb database which is able to connect to the AWS (S3) account.
        Have to be used as context manager

        Returns:
            DuckDBPyConnection
        """
        # Connect to S3 via DuckDB.
        duckdb_conn = duckdb.connect(":memory:")

        # Configure mandatory credentials.
        duckdb_conn.execute(f"SET s3_access_key_id='{self.connection_data['aws_access_key_id']}'")
        duckdb_conn.execute(f"SET s3_secret_access_key='{self.connection_data['aws_secret_access_key']}'")

        # Configure optional parameters.
        if 'aws_session_token' in self.connection_data:
            duckdb_conn.execute(f"SET s3_session_token='{self.connection_data['aws_session_token']}'")

        if 'region_name' in self.connection_data:
            duckdb_conn.execute(f"SET s3_region='{self.connection_data['region_name']}'")

        try:
            yield duckdb_conn
        finally:
            duckdb_conn.close()

    def _connect_boto3(self) -> boto3.client:
        """
        Establishes a connection to the AWS (S3) account.

        Returns:
            boto3.client: A client object to the AWS (S3) account.
        """
        # Configure mandatory credentials.
        config = {
            'aws_access_key_id': self.connection_data['aws_access_key_id'],
            'aws_secret_access_key': self.connection_data['aws_secret_access_key']
        }

        # Configure optional parameters.
        if 'aws_session_token' in self.connection_data:
            config['aws_session_token'] = self.connection_data['aws_session_token']

        # DuckDB considers us-east-1 to be the default region.
        config['region_name'] = self.connection_data['region_name'] if 'region_name' in self.connection_data else 'us-east-1'

        client = boto3.client('s3', **config)

        result = client.head_bucket(Bucket=self.connection_data['bucket'])

        # Check if the bucket is in the same region as specified: the DuckDB connection will fail otherwise.
        if result['ResponseMetadata']['HTTPHeaders']['x-amz-bucket-region'] != client.meta.region_name:
            raise ValueError('The bucket is not in the expected region.')

        return client

    def disconnect(self):
        """
        Closes the connection to the AWS (S3) account if it's currently open.
        """
        if not self.is_connected:
            return
        self.connection.close()
        self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the S3 bucket.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        # Check connection via boto3.
        try:
            self._connect_boto3()
            response.success = True
        except (ClientError, ValueError) as e:
            logger.error(f'Error connecting to S3 with the given credentials, {e}!')
            response.error_message = str(e)

        if response.success and need_to_close:
            self.disconnect()

        elif not response.success and self.is_connected:
            self.is_connected = False

        return response

    def _read_as_table(self, key) -> pd.DataFrame:
        """
        Read object as dataframe. Uses duckdb
        """

        with self._connect_duckdb() as connection:

            cursor = connection.execute(f"SELECT * FROM 's3://{self.connection_data['bucket']}/{key}'")
            return cursor.fetchdf()

    def _read_as_content(self, key) -> None:
        """
        Read object as content
        """

        client = self.connect()

        obj = client.get_object(Bucket=self.connection_data['bucket'], Key=key)
        content = obj['Body'].read()
        return content

    def _add_data_to_table(self, key, df) -> None:
        """
        Writes the table to a file in the S3 bucket.

        Raises:
            CatalogException: If the table does not exist in the DuckDB connection.
        """

        # Check if the file exists in the S3 bucket.
        try:
            client = self.connect()
            client.head_object(Bucket=self.connection_data['bucket'], Key=key)
        except ClientError as e:
            logger.error(f'Error querying the file {key} in the bucket {self.connection_data["bucket"]}, {e}!')
            raise e

        with self._connect_duckdb() as connection:
            # copy
            connection.execute(f"CREATE TABLE tmp_table AS SELECT * FROM 's3://{self.connection_data['bucket']}/{key}'")

            # insert
            connection.execute("INSERT INTO tmp_table BY NAME SELECT * FROM df")

            # upload
            connection.execute(f"COPY tmp_table TO 's3://{self.connection_data['bucket']}/{key}'")

    def query(self, query: ASTNode) -> Response:
        """
        Executes a SQL query represented by an ASTNode and retrieves the data.

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.

        Raises:
            ValueError: If the file format is not supported or the file does not exist in the S3 bucket.

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

    def _get_tables(self) -> List[str]:
        client = self.connect()
        objects = client.list_objects_v2(Bucket=self.connection_data["bucket"])['Contents']

        # Get only the supported file formats.
        # Wrap the object names with backticks to prevent SQL syntax errors.
        supported_objects = [f"`{obj['Key']}`" for obj in objects if obj['Key'].split('.')[-1] in self.supported_file_formats]
        return supported_objects

    def get_tables(self) -> Response:
        """
        Retrieves a list of tables (objects) in the S3 bucket.

        Each object is considered a table. Only the supported file formats are considered as tables.

        Returns:
            Response: A response object containing the list of tables and views, formatted as per the `Response` class.
        """
        supported_objects = self._get_tables()

        # virtual table with list of files
        supported_objects.insert(0, 'files')

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
        Retrieves column details for a specified table (object) in the S3 bucket.

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
                    'data_type': [data_type if data_type != 'object' else 'string' for data_type in result.data_frame.dtypes]
                }
            )
        )

        return response
