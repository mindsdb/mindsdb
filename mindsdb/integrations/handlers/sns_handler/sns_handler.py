
from mindsdb.integrations.handlers.sns_handler.sns_tables import TopicTable
from mindsdb.integrations.handlers.sns_handler.sns_tables import MessageTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb_sql import parse_sql
from collections import OrderedDict
from typing import Optional
import boto3
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
import json as JSON
from mindsdb.utilities import log


class SnsHandler(APIHandler):
    """
    This handler handles connection and execution of the sns statements.
    """

    name = 'sns'
    connection = None

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs) -> None:
        """initializer method

        Args:
            name (str): handler name
        """
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = "sns"
        self.connection_data = connection_data 
        self.kwargs = kwargs
        self.connection = None
        self.is_connected = False

        _tables = [
            TopicTable, MessageTable
        ]

        for Table in _tables:
            self._register_table(Table.name, Table(self))

    def check_connection(self) -> StatusResponse:
        """checking the connection

        Returns:
            StatusResponse: whether the connection is still up
        """
        response = StatusResponse(False)
        try:
            self.connection.list_topics()
            response = StatusResponse(True)
        except Exception as e:
            log.logger.error(f'Error connecting to AWS with the given credentials, {e}!')
        return response
    
    def disconnect(self):
        """ Close any existing connections
        Should switch self.is_connected.
        """
        self.is_connected = False
        return

    def connect(self):
        """making the connectino object
        """
        if self.is_connected is True:
            return self.connection
        if self.connection_data['endpoint_url'] is not None:
            self.connection = boto3.client(
                'sns',
                aws_access_key_id=self.connection_data['aws_access_key_id'],
                aws_secret_access_key=self.connection_data['aws_secret_access_key'],
                region_name=self.connection_data['region_name'],
                endpoint_url="http://localstack:4566"
            )
        else:
            self.connection = boto3.client(
                'sns',
                aws_access_key_id=self.connection_data['aws_access_key_id'],
                aws_secret_access_key=self.connection_data['aws_secret_access_key'],
                region_name=self.connection_data['region_name'])                
        self.is_connected = True
        return self.connection

    def topic_list(self, params: dict = None):
        json_response = str(self.connection.list_topics())
        json_response = json_response.replace("\'", "\"")
        data = JSON.loads(str(json_response))
        return data["Topics"]
    
    def publish_message(self,  topic_arn = None, message = None):
        self.connection.publish(TopicArn=topic_arn,Message=message)
        
    def create_topic(self, name = None):
        self.connection.create_topic(Name=name)
        
    def native_query(self, query: str) -> StatusResponse:
        """Receive and process a raw query.

        Parameters
        ----------
        query : str
            query in a native format

        Returns
        -------
        StatusResponse
            Request status
        """
        ast = parse_sql(query, dialect="mindsdb")
        return self.query(ast)


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
    })