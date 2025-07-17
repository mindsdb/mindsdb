from typing import List
from contextlib import contextmanager

import boto3
import duckdb
from duckdb import HTTPException
from mindsdb_sql_parser import parse_sql
import pandas as pd
from typing import Text, Dict, Optional
from botocore.client import Config
from botocore.exceptions import ClientError

from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb_sql_parser.ast import Select, Identifier, Insert, Star, Constant

from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)

from mindsdb.integrations.libs.api_handler import APIResource, APIHandler
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator

logger = log.getLogger(__name__)


class ListFilesTable(APIResource):
    def list(
        self, targets: List[str] = None, conditions: List[FilterCondition] = None, limit: int = None, *args, **kwargs
    ) -> pd.DataFrame:
        buckets = None
        for condition in conditions:
            if condition.column == "bucket":
                if condition.op == FilterOperator.IN:
                    buckets = condition.value
                elif condition.op == FilterOperator.EQUAL:
                    buckets = [condition.value]
                condition.applied = True

        data = []
        for obj in self.handler.get_objects(limit=limit, buckets=buckets):
            path = obj["Key"]
            path = path.replace("`", "")
            item = {
                "path": path,
                "bucket": obj["Bucket"],
                "name": path[path.rfind("/") + 1 :],
                "extension": path[path.rfind(".") + 1 :],
            }

            if targets and "public_url" in targets:
                item["public_url"] = self.handler.generate_sas_url(path, obj["Bucket"])

            data.append(item)

        return pd.DataFrame(data=data, columns=self.get_columns())

    def get_columns(self) -> List[str]:
        return ["path", "name", "extension", "bucket", "content", "public_url"]


class FileTable(APIResource):
    def list(self, targets: List[str] = None, table_name=None, *args, **kwargs) -> pd.DataFrame:
        return self.handler.read_as_table(table_name)

    def add(self, data, table_name=None):
        df = pd.DataFrame(data)
        return self.handler.add_data_to_table(table_name, df)


class S3Handler(APIHandler):
    """
    This handler handles connection and execution of the SQL statements on AWS S3.
    """

    name = "s3"
    # TODO: Can other file formats be supported?
    supported_file_formats = ["csv", "tsv", "json", "parquet"]

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
        self.bucket = self.connection_data.get("bucket")
        self._regions = {}

        self._files_table = ListFilesTable(self)

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self):
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
        if not all(key in self.connection_data for key in ["aws_access_key_id", "aws_secret_access_key"]):
            raise ValueError("Required parameters (aws_access_key_id, aws_secret_access_key) must be provided.")

        # Connect to S3 and configure mandatory credentials.
        self.connection = self._connect_boto3()
        self.is_connected = True

        return self.connection

    @contextmanager
    def _connect_duckdb(self, bucket):
        """
        Creates temporal duckdb database which is able to connect to the AWS (S3) account.
        Have to be used as context manager

        Returns:
            DuckDBPyConnection
        """
        # Connect to S3 via DuckDB.
        duckdb_conn = duckdb.connect(":memory:")
        try:
            duckdb_conn.execute("INSTALL httpfs")
        except HTTPException as http_error:
            logger.debug(f"Error installing the httpfs extension, {http_error}! Forcing installation.")
            duckdb_conn.execute("FORCE INSTALL httpfs")

        duckdb_conn.execute("LOAD httpfs")

        # Configure mandatory credentials.
        duckdb_conn.execute(f"SET s3_access_key_id='{self.connection_data['aws_access_key_id']}'")
        duckdb_conn.execute(f"SET s3_secret_access_key='{self.connection_data['aws_secret_access_key']}'")

        # Configure optional parameters.
        if "aws_session_token" in self.connection_data:
            duckdb_conn.execute(f"SET s3_session_token='{self.connection_data['aws_session_token']}'")

        # detect region for bucket
        if bucket not in self._regions:
            client = self.connect()
            self._regions[bucket] = client.get_bucket_location(Bucket=bucket)["LocationConstraint"]

        region = self._regions[bucket]
        duckdb_conn.execute(f"SET s3_region='{region}'")

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
            "aws_access_key_id": self.connection_data["aws_access_key_id"],
            "aws_secret_access_key": self.connection_data["aws_secret_access_key"],
        }

        # Configure optional parameters.
        optional_parameters = ["region_name", "aws_session_token"]
        for parameter in optional_parameters:
            if parameter in self.connection_data:
                config[parameter] = self.connection_data[parameter]

        client = boto3.client("s3", **config, config=Config(signature_version="s3v4"))

        # check connection
        if self.bucket is not None:
            client.head_bucket(Bucket=self.bucket)
        else:
            client.list_buckets()

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
            logger.error(f"Error connecting to S3 with the given credentials, {e}!")
            response.error_message = str(e)

        if response.success and need_to_close:
            self.disconnect()

        elif not response.success and self.is_connected:
            self.is_connected = False

        return response

    def _get_bucket(self, key):
        if self.bucket is not None:
            return self.bucket, key

        # get bucket from first part of the key
        ar = key.split("/")
        return ar[0], "/".join(ar[1:])

    def read_as_table(self, key) -> pd.DataFrame:
        """
        Read object as dataframe. Uses duckdb
        """
        bucket, key = self._get_bucket(key)

        with self._connect_duckdb(bucket) as connection:
            cursor = connection.execute(f"SELECT * FROM 's3://{bucket}/{key}'")

            return cursor.fetchdf()

    def _read_as_content(self, key) -> None:
        """
        Read object as content
        """
        bucket, key = self._get_bucket(key)

        client = self.connect()

        obj = client.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read()
        return content

    def add_data_to_table(self, key, df) -> None:
        """
        Writes the table to a file in the S3 bucket.

        Raises:
            CatalogException: If the table does not exist in the DuckDB connection.
        """

        # Check if the file exists in the S3 bucket.
        bucket, key = self._get_bucket(key)

        try:
            client = self.connect()
            client.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            logger.error(f"Error querying the file {key} in the bucket {bucket}, {e}!")
            raise e

        with self._connect_duckdb(bucket) as connection:
            # copy
            connection.execute(f"CREATE TABLE tmp_table AS SELECT * FROM 's3://{bucket}/{key}'")

            # insert
            connection.execute("INSERT INTO tmp_table BY NAME SELECT * FROM df")

            # upload
            connection.execute(f"COPY tmp_table TO 's3://{bucket}/{key}'")

    def query(self, query: ASTNode) -> Response:
        """
        Executes a SQL query represented by an ASTNode and retrieves the data.

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.

        Raises:
            ValueError: If the file format is not supported or the file does not exist in the S3 bucket.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """

        self.connect()

        if isinstance(query, Select):
            table_name = query.from_table.parts[-1]

            if table_name == "files":
                table = self._files_table
                df = table.select(query)

                # add content
                has_content = False
                for target in query.targets:
                    if isinstance(target, Identifier) and target.parts[-1].lower() == "content":
                        has_content = True
                        break
                if has_content:
                    df["content"] = df["path"].apply(self._read_as_content)
            else:
                extension = table_name.split(".")[-1]
                if extension not in self.supported_file_formats:
                    logger.error(f"The file format {extension} is not supported!")
                    raise ValueError(f"The file format {extension} is not supported!")

                table = FileTable(self, table_name=table_name)
                df = table.select(query)

            response = Response(RESPONSE_TYPE.TABLE, data_frame=df)
        elif isinstance(query, Insert):
            table_name = query.table.parts[-1]
            table = FileTable(self, table_name=table_name)
            table.insert(query)
            response = Response(RESPONSE_TYPE.OK)
        else:
            raise NotImplementedError

        return response

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

    def get_objects(self, limit=None, buckets=None) -> List[dict]:
        client = self.connect()
        if self.bucket is not None:
            add_bucket_to_name = False
            scan_buckets = [self.bucket]
        else:
            add_bucket_to_name = True
            scan_buckets = [b["Name"] for b in client.list_buckets()["Buckets"]]

        objects = []
        for bucket in scan_buckets:
            if buckets is not None and bucket not in buckets:
                continue

            resp = client.list_objects_v2(Bucket=bucket)
            if "Contents" not in resp:
                continue

            for obj in resp["Contents"]:
                if obj.get("StorageClass", "STANDARD") != "STANDARD":
                    continue

                obj["Bucket"] = bucket
                if add_bucket_to_name:
                    # bucket is part of the name
                    obj["Key"] = f"{bucket}/{obj['Key']}"
                objects.append(obj)
            if limit is not None and len(objects) >= limit:
                break

        return objects

    def generate_sas_url(self, key: str, bucket: str) -> str:
        """
        Generates a pre-signed URL for accessing an object in the S3 bucket.

        Args:
            key (str): The key (path) of the object in the S3 bucket.
            bucket (str): The name of the S3 bucket.

        Returns:
            str: The pre-signed URL for accessing the object.
        """
        client = self.connect()
        url = client.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=3600)
        return url

    def get_tables(self) -> Response:
        """
        Retrieves a list of tables (objects) in the S3 bucket.

        Each object is considered a table. Only the supported file formats are considered as tables.

        Returns:
            Response: A response object containing the list of tables and views, formatted as per the `Response` class.
        """

        # Get only the supported file formats.
        # Wrap the object names with backticks to prevent SQL syntax errors.
        supported_names = [
            f"`{obj['Key']}`" for obj in self.get_objects() if obj["Key"].split(".")[-1] in self.supported_file_formats
        ]

        # virtual table with list of files
        supported_names.insert(0, "files")

        response = Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame(supported_names, columns=["table_name"]))

        return response

    def get_columns(self, table_name: str) -> Response:
        """
        Retrieves column details for a specified table (object) in the S3 bucket.

        Args:
            table_name (Text): The name of the table for which to retrieve column information.

        Raises:
            ValueError: If the 'table_name' is not a valid string.

        Returns:
            Response: A response object containing the column details, formatted as per the `Response` class.
        """
        query = Select(targets=[Star()], from_table=Identifier(parts=[table_name]), limit=Constant(1))

        result = self.query(query)

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                {
                    "column_name": result.data_frame.columns,
                    "data_type": [
                        data_type if data_type != "object" else "string" for data_type in result.data_frame.dtypes
                    ],
                }
            ),
        )

        return response
