from typing import Optional

import pandas as pd
import boto3
from botocore.exceptions import ClientError
import io
import ast

from mindsdb.integrations.libs.base import DatabaseHandler

from mindsdb_sql.parser.ast import Select
from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)


logger = log.getLogger(__name__)

class S3Handler(DatabaseHandler):
    """
    This handler handles connection and execution of the S3 statements.
    """

    name = 's3'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs
        self.table_name = None

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> boto3.client:
        """
        Set up the connection required by the handler.
        Returns:
            HandlerStatusResponse
        """

        if self.is_connected is True:
            return self.connection
        
        # Mandatory connection parameters.
        if not all(key in self.connection_data for key in ['aws_access_key_id', 'aws_secret_access_key', 'bucket']):
            raise ValueError('Required parameters (aws_access_key_id, aws_secret_access_key, bucket) must be provided.')

        config = {
            'aws_access_key_id': self.connection_data.get('aws_access_key_id'),
            'aws_secret_access_key': self.connection_data.get('aws_secret_access_key'),
        }

        # Optional connection parameters.
        optional_params = ['aws_session_token', 'region_name']
        for param in optional_params:
            if param in self.connection_data:
                config[param] = self.connection_data[param]

        self.connection = boto3.client(
            's3',
            **config
        )
        self.is_connected = True

        return self.connection

    def disconnect(self) -> None:
        """ Close any existing connections
        Should switch self.is_connected.
        """
        self.is_connected = False
        return

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """

        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()
            connection.head_bucket(Bucket=self.connection_data['bucket'])
            response.success = True
        except ClientError as e:
            logger.error(f'Error connecting to AWS with the given credentials, {e}!')
            response.error_message = str(e)

        if response.success and need_to_close:
            self.disconnect()

        elif not response.success and self.is_connected:
            self.is_connected = False

        return response

    def native_query(self, query: str) -> Response:
        """
        Receive raw query and act upon it somehow.
        Args:
            query (str): query in native format
        Returns:
            HandlerResponse
        """

        need_to_close = self.is_connected is False

        connection = self.connect()

        try:
            result = connection.select_object_content(
                Bucket=self.connection_data['bucket'],
                Key=self.connection_data['key'],
                ExpressionType='SQL',
                Expression=query,
                InputSerialization=ast.literal_eval(self.connection_data['input_serialization']),
                OutputSerialization={"CSV": {}}
            )

            records = []
            for event in result['Payload']:
                if 'Records' in event:
                    records.append(event['Records']['Payload'])
                elif 'Stats' in event:
                    stats = event['Stats']['Details']

            file_str = ''.join(r.decode('utf-8') for r in records)

            df = pd.read_csv(io.StringIO(file_str))

            response = Response(
                RESPONSE_TYPE.TABLE,
                data_frame=df
            )
        except Exception as e:
            logger.error(f'Error running query: {query} on {self.connection_data["key"]} in {self.connection_data["bucket"]}!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INTSERT, DELETE, etc
        Returns:
            HandlerResponse
        """

        if not isinstance(query, Select):
            raise ValueError('Only SELECT queries are supported.')
        
        self.table_name = query.from_table
        query.formatter = 'S3Object'

        return self.native_query(query.to_string())

    def get_tables(self) -> Response:
        """
        Return list of entities that will be accessible as tables.
        Returns:
            HandlerResponse
        """

        connection = self.connect()
        objects = connection.list_objects(Bucket=self.connection_data["bucket"])['Contents']

        # Get only CSV, JSON, and Parquet files.
        # Only these formats are supported select_object_content.
        # Replace the period with an underscore to allow them to be used as table names.
        supported_objects = [obj['Key'].replace('.', '_') for obj in objects if obj['Key'].split('.')[-1] in ['csv', 'json', 'parquet']]

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                supported_objects,
                columns=['table_name']
            )
        )

        return response

    def get_columns(self, table_name) -> Response:
        """
        Returns a list of entity columns.
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            HandlerResponse
        """

        query = f"SELECT * FROM {table_name} LIMIT 5"
        result = self.native_query(query)

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                {
                    'column_name': result.data_frame.columns,
                    'data_type': result.data_frame.dtypes
                }
            )
        )

        return response
