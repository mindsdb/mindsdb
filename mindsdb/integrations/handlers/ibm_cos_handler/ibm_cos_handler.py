from contextlib import contextmanager
from typing import Text, Dict, Optional, List

import ibm_boto3
from ibm_botocore.client import ClientError
import pandas as pd
import duckdb

from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.ast import Select, Identifier, Insert, Star, Constant

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
        self,
        targets: List[str] = None,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        *args,
        **kwargs,
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
            if obj["Filename"].split(".")[1] in self.handler.supported_file_formats:
                item = {
                    "path": path,
                    "bucket": obj["Bucket"],
                    "name": path[path.rfind("/") + 1:],
                    "extension": path[path.rfind(".") + 1:],
                }

                data.append(item)

        return pd.DataFrame(data=data, columns=self.get_columns())

    def get_columns(self) -> List[str]:
        return ["path", "name", "extension", "bucket", "content"]


class FileTable(APIResource):
    def list(
        self, targets: List[str] = None, table_name=None, *args, **kwargs
    ) -> pd.DataFrame:
        return self.handler.read_as_table(table_name)

    def add(self, data, table_name=None):
        df = pd.DataFrame(data)
        return self.handler.add_data_to_table(table_name, df)


class IBMCloudObjectStorageHandler(APIHandler):

    name = "ibm_cos"
    supported_file_formats = ["csv", "tsv", "json", "parquet"]

    def __init__(self, name: Text, connection_data: Optional[Dict] = None, **kwargs):
        super().__init__(name)
        self.connection_data = connection_data or {}
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False
        self.thread_safe = True
        self._regions = {}

        self.bucket = self.connection_data.get("bucket")
        self._files_table = ListFilesTable(self)

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self):
        if self.is_connected is True:
            return self.connection

        required_params = [
            "cos_hmac_access_key_id",
            "cos_hmac_secret_access_key",
            "cos_endpoint_url",
        ]
        if not all(key in self.connection_data for key in required_params):
            raise ValueError(
                "Required parameters (cos_hmac_access_key_id, cos_hmac_secret_access_key, cos_endpoint_url) must be provided."
            )

        self.connection = self._connect_ibm_boto3()
        self.is_connected = True

        return self.connection

    def _connect_ibm_boto3(self) -> ibm_boto3.client:
        config = {
            "aws_access_key_id": self.connection_data["cos_hmac_access_key_id"],
            "aws_secret_access_key": self.connection_data["cos_hmac_secret_access_key"],
            "endpoint_url": self.connection_data["cos_endpoint_url"],
        }

        client = ibm_boto3.client("s3", **config)

        if self.bucket is not None:
            client.head_bucket(Bucket=self.bucket)
        else:
            client.list_buckets()

        return client

    def disconnect(self):
        if not self.is_connected:
            return
        self.connection = None
        self.is_connected = False

    def check_connection(self) -> StatusResponse:
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self._connect_ibm_boto3()
            response.success = True
        except (ClientError, ValueError) as e:
            logger.error(
                f"Error connecting to IBM COS with the given credentials, {e}!"
            )
            response.error_message = str(e)

        if response.success and need_to_close:
            self.disconnect()

        elif not response.success and self.is_connected:
            self.is_connected = False

        return response

    @contextmanager
    def _connect_duckdb(self):
        duckdb_conn = duckdb.connect(":memory:")
        duckdb_conn.execute("INSTALL httpfs")
        duckdb_conn.execute("LOAD httpfs")

        duckdb_conn.execute(
            f"SET s3_access_key_id='{self.connection_data['cos_hmac_access_key_id']}'"
        )
        duckdb_conn.execute(
            f"SET s3_secret_access_key='{self.connection_data['cos_hmac_secret_access_key']}'"
        )

        endpoint_url = self.connection_data["cos_endpoint_url"]
        if endpoint_url.startswith("https://"):
            endpoint_url = endpoint_url[len("https://"):]
        elif endpoint_url.startswith("http://"):
            endpoint_url = endpoint_url[len("http://"):]

        duckdb_conn.execute(f"SET s3_endpoint='{endpoint_url}'")
        duckdb_conn.execute("SET s3_url_style='path'")
        duckdb_conn.execute("SET s3_use_ssl=true")

        try:
            yield duckdb_conn
        finally:
            duckdb_conn.close()

    def _get_bucket(self, key):
        if self.bucket is not None:
            return self.bucket, key

        ar = key.split("/")
        return ar[0], "/".join(ar[1:])

    def read_as_table(self, key) -> pd.DataFrame:
        bucket, key = self._get_bucket(key)

        with self._connect_duckdb() as connection:

            cursor = connection.execute(f"SELECT * FROM 's3://{bucket}/{key}'")

            return cursor.fetchdf()

    def _read_as_content(self, key) -> None:
        bucket, key = self._get_bucket(key)

        client = self.connect()

        obj = client.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read()
        return content

    def add_data_to_table(self, key, df) -> None:

        bucket, key = self._get_bucket(key)

        try:
            client = self.connect()
            client.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            logger.error(f"Error querying the file {key} in the bucket {bucket}, {e}!")
            raise e

        with self._connect_duckdb() as connection:
            connection.execute(
                f"CREATE TABLE tmp_table AS SELECT * FROM 's3://{bucket}/{key}'"
            )

            connection.execute("INSERT INTO tmp_table BY NAME SELECT * FROM df")

            connection.execute(f"COPY tmp_table TO 's3://{bucket}/{key}'")

    def query(self, query: ASTNode) -> Response:
        self.connect()
        if isinstance(query, Select):
            table_name = query.from_table.parts[-1]

            if table_name == "files":
                table = self._files_table
                df = table.select(query)

                has_content = False
                for target in query.targets:
                    if (
                        isinstance(target, Identifier)
                        and target.parts[-1].lower() == "content"
                    ):
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

    def get_objects(self, limit=None, buckets=None) -> List[dict]:
        client = self.connect()
        if self.bucket is not None:
            add_bucket_to_name = False
            scan_buckets = [self.bucket]
        else:
            add_bucket_to_name = True
            resp = client.list_buckets()
            scan_buckets = [b["Name"] for b in resp["Buckets"]]

        objects = []
        for bucket in scan_buckets:
            if buckets is not None and bucket not in buckets:
                continue

            resp = client.list_objects_v2(Bucket=bucket)
            if "Contents" not in resp:
                continue

            for obj in resp["Contents"]:
                obj["Bucket"] = bucket
                obj["Filename"] = obj["Key"]
                if add_bucket_to_name:
                    obj["Key"] = f'{bucket}/{obj["Key"]}'
                objects.append(obj)
            if limit is not None and len(objects) >= limit:
                break

        return objects

    def get_tables(self) -> Response:
        supported_names = [
            f"{obj['Key']}"
            for obj in self.get_objects()
            if obj["Key"].split(".")[-1] in self.supported_file_formats
        ]

        supported_names.insert(0, "files")

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(supported_names, columns=["table_name"]),
        )

        return response

    def get_columns(self, table_name: str) -> Response:
        query = Select(
            targets=[Star()],
            from_table=Identifier(parts=[table_name]),
            limit=Constant(1),
        )

        result = self.query(query)

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                {
                    "column_name": result.data_frame.columns,
                    "data_type": [
                        str(dtype) if str(dtype) != "object" else "string"
                        for dtype in result.data_frame.dtypes
                    ],
                }
            ),
        )

        return response
