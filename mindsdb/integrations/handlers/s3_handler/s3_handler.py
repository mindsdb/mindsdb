from typing import Optional
from collections import OrderedDict

import pandas as pd
import boto3
import io
import ast

from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.libs.base import DatabaseHandler

from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


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
        self.parser = parse_sql
        self.dialect = 's3'
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> StatusResponse:
        """
        Set up the connection required by the handler.
        Returns:
            HandlerStatusResponse
        """

        if self.is_connected is True:
            return self.connection

        self.connection = boto3.client(
            's3',
            aws_access_key_id=self.connection_data['aws_access_key_id'],
            aws_secret_access_key=self.connection_data['aws_secret_access_key'],
            region_name=self.connection_data['region_name']
        )
        self.is_connected = True

        return self.connection

    def disconnect(self):
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
            self.connect()
            response.success = True
        except Exception as e:
            log.logger.error(f'Error connecting to AWS with the given credentials, {e}!')
            response.error_message = str(e)
        finally:
            if response.success is True and need_to_close:
                self.disconnect()
            if response.success is False and self.is_connected is True:
                self.is_connected = False

        return response

    def native_query(self, query: str) -> StatusResponse:
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
            log.logger.error(f'Error running query: {query} on {self.connection_data["key"]} in {self.connection_data["bucket"]}!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> StatusResponse:
        """
        Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INTSERT, DELETE, etc
        Returns:
            HandlerResponse
        """

        return self.native_query(query.to_string())

    def get_tables(self) -> StatusResponse:
        """
        Return list of entities that will be accessible as tables.
        Returns:
            HandlerResponse
        """

        connection = self.connect()
        objects = [obj['Key'] for obj in connection.list_objects(Bucket=self.connection_data["bucket"])['Contents']]

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                objects,
                columns=['table_name']
            )
        )

        return response

    def get_columns(self) -> StatusResponse:
        """
        Returns a list of entity columns.
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            HandlerResponse
        """

        query = "SELECT * FROM S3Object LIMIT 5"
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


connection_args = OrderedDict(
    aws_access_key_id={
        'type': ARG_TYPE.STR,
        'description': 'The access key for the AWS account.'
    },
    aws_secret_access_key={
        'type': ARG_TYPE.STR,
        'description': 'The secret key for the AWS account.'
    },
    region_name={
        'type': ARG_TYPE.STR,
        'description': 'The AWS region where the S3 bucket is located.'
    },
    bucket={
        'type': ARG_TYPE.STR,
        'description': 'The name of the S3 bucket.'
    },
    key={
        'type': ARG_TYPE.STR,
        'description': 'The key of the object to be queried.'
    },
    input_serialization={
        'type': ARG_TYPE.STR,
        'description': 'The format of the data in the object that is to be queried.'
    }
)

connection_args_example = OrderedDict(
    aws_access_key_id='PCAQ2LJDOSWLNSQKOCPW',
    aws_secret_access_key='U/VjewPlNopsDmmwItl34r2neyC6WhZpUiip57i',
    region_name='us-east-1',
    bucket='mindsdb-bucket',
    key='iris.csv',
    input_serialization="{'CSV': {'FileHeaderInfo': 'NONE'}}",
)


