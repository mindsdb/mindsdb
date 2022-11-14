from typing import Optional
from collections import OrderedDict

import pandas as pd
import boto3
from boto3.dynamodb.types import TypeDeserializer

from mindsdb_sql import parse_sql

from mindsdb.integrations.libs.base import DatabaseHandler

from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


class DyanmoDBHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the DynamoDB statements.
    """

    name = 'dynamodb'

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
        self.dialect = 'dynamodb'

        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def connect(self) -> StatusResponse:
        """
        Set up the connection required by the handler.
        Returns:
            HandlerStatusResponse
        """

        if self.is_connected is True:
            return self.connection

        self.connection = boto3.client(
            'dynamodb',
            aws_access_key_id=self.connection_data['aws_access_key_id'],
            aws_secret_access_key=self.connection_data['aws_secret_access_key'],
            region_name=self.connection_data['region_name']
        )

        self.is_connected = True

        return self.connection

    def disconnect(self):
        """
        Close any existing connections.
        """

        if self.is_connected is False:
            return

        self.connection.close()
        self.is_connected = False
        return self.is_connected

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
            log.logger.error(f'Error connecting to DynamoDB, {e}!')
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
            result = connection.execute_statement(Statement=query)
            if result['Items']:
                records = []
                for record in result['Items']:
                    records.append(self.parse_record(record))
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=pd.json_normalize(records)
                )
            else:
                response = Response(RESPONSE_TYPE.OK)
        except Exception as e:
            log.logger.error(f'Error running query: {query} on DynamoDB!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

        connection.close()
        if need_to_close is True:
            self.disconnect()

        return response

    def parse_record(self, record):
        deserializer = TypeDeserializer()
        return {k: deserializer.deserialize(v) for k,v in record.items()}

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

        result = self.connection.list_tables()

        df = pd.DataFrame(
            data=result['TableNames'],
            columns=['table_name']
        )

        response = Response(
            RESPONSE_TYPE.TABLE,
            df
        )

        return response

    def get_columns(self, table_name: str) -> StatusResponse:
        """
        Returns a list of entity columns.
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            HandlerResponse
        """

        result = self.connection.describe_table(
            TableName=table_name
        )

        df = pd.DataFrame(
            result['Table']['AttributeDefinitions']
        )

        df = df.rename(
            columns={
                'AttributeName': 'column_name',
                'AttributeType': 'data_type'
            }
        )

        response = Response(
            RESPONSE_TYPE.TABLE,
            df
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
        'description': 'The AWS region where the DynamoDB tables are created.'
    }
)

connection_args_example = OrderedDict(
    aws_access_key_id='PCAQ2LJDOSWLNSQKOCPW',
    aws_secret_access_key='U/VjewPlNopsDmmwItl34r2neyC6WhZpUiip57i',
    region_name='us-east-1'
)