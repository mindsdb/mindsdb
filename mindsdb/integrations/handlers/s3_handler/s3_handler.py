from typing import List
from contextlib import contextmanager
from io import BytesIO

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
from mindsdb.integrations.utilities.files.file_reader import FileReader

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
    # Structured formats use DuckDB, text formats use FileReader
    supported_file_formats = ["csv", "tsv", "json", "parquet", "txt", "pdf", "md", "doc", "docx"]
    text_file_formats = ["txt", "pdf", "md", "doc", "docx"]

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

        self.session = None
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

        Returns:
            boto3.client: A client object to the AWS (S3) account.
        """
        if self.is_connected is True:
            return self.connection

        # Se as credenciais não forem passadas, tenta conectar usando o ambiente (IAM Role via ServiceAccount)
        if not ("aws_access_key_id" in self.connection_data and "aws_secret_access_key" in self.connection_data):
            self.connection = self._connect_boto3(use_env_credentials=True)
        else:
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

        duckdb_conn.execute("LOAD httpfs")

        # detect region for bucket
        if bucket not in self._regions:
            client = self.connect()
            bucket_region = client.get_bucket_location(Bucket=bucket).get("LocationConstraint")
            # LocationConstraint pode ser None para us-east-1
            self._regions[bucket] = bucket_region or "us-east-1"

        region = self.connection_data.get("region_name", self._regions.get(bucket, "us-east-1"))
        region = region or "us-east-1"

        credentials = None
        if self.session is not None:
            credentials = self.session.get_credentials()

        if credentials is not None:
            frozen_credentials = credentials.get_frozen_credentials()
            access_key = frozen_credentials.access_key
            secret_key = frozen_credentials.secret_key
            session_token = frozen_credentials.token

            if access_key:
                duckdb_conn.execute("SET s3_access_key_id=?", [access_key])
            if secret_key:
                duckdb_conn.execute("SET s3_secret_access_key=?", [secret_key])
            if session_token:
                duckdb_conn.execute("SET s3_session_token=?", [session_token])
        else:
            # Cria o secret S3 para credential_chain (IAM Role, env, etc)
            safe_region = region.replace("'", "''")
            duckdb_conn.execute(
                f"""
                CREATE OR REPLACE SECRET default_s3 (
                    TYPE s3,
                    PROVIDER credential_chain,
                    REGION '{safe_region}'
                );
                """
            )

        duckdb_conn.execute("SET s3_region=?", [region])

        try:
            yield duckdb_conn
        finally:
            duckdb_conn.close()

    def _connect_boto3(self, use_env_credentials=False) -> boto3.client:
        """
        Establishes a connection to the AWS (S3) account.

        Returns:
            boto3.client: A client object to the AWS (S3) account.
        """
        session_kwargs = {}
        # Se use_env_credentials ou não existem as chaves, usa só region_name se existir
        if use_env_credentials or not ("aws_access_key_id" in self.connection_data and "aws_secret_access_key" in self.connection_data):
            if "region_name" in self.connection_data:
                session_kwargs["region_name"] = self.connection_data["region_name"]
            # Não adiciona credenciais, boto3 usará IAM Role do ambiente
        else:
            # Configure mandatory credentials.
            session_kwargs["aws_access_key_id"] = self.connection_data["aws_access_key_id"]
            session_kwargs["aws_secret_access_key"] = self.connection_data["aws_secret_access_key"]
            # Configure optional parameters.
            optional_parameters = ["region_name", "aws_session_token"]
            for parameter in optional_parameters:
                if parameter in self.connection_data:
                    session_kwargs[parameter] = self.connection_data[parameter]

        self.session = boto3.Session(**session_kwargs)
        client = self.session.client("s3", config=Config(signature_version="s3v4"))

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
        self.session = None
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
        Read object as dataframe. Uses duckdb for structured files, FileReader for text files
        """
        bucket, key = self._get_bucket(key)

        # Check if file is a text format
        extension = key.split(".")[-1].lower()
        if extension in self.text_file_formats:
            # Use FileReader for text files
            content = self._read_as_content(key)
            file_obj = BytesIO(content)

            # Extract filename from key
            file_name = key.split("/")[-1]

            # Use FileReader to parse the content
            file_reader = FileReader(file=file_obj, name=file_name)
            tables = file_reader.get_contents()

            # Return the main table (text files have single table)
            return tables.get("main", pd.DataFrame())
        else:
            # Use DuckDB for structured files (csv, json, parquet)
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
