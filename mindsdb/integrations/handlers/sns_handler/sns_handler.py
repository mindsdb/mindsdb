from mindsdb.integrations.handlers.sns_handler.sns_tables import TopicTable
from mindsdb.integrations.handlers.sns_handler.sns_tables import MessageTable
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb_sql import parse_sql
from pandas import DataFrame
from collections import OrderedDict
from typing import Optional
import boto3
from typing import Dict
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
import json as JSON
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.api_handler import APIHandler,FuncParser
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
        self.connection = boto3.client(
                'sns',
                aws_access_key_id=self.connection_data['aws_access_key_id'],
                aws_secret_access_key=self.connection_data['aws_secret_access_key'],
                # verify=False,
                # using  for testing locally with localstack
                # endpoint_url=self.connection_data['endpoint_url'],
                region_name=self.connection_data['region_name'])     
        self.is_connected = True
        return self.connection

    def topic_list(self, params: Dict = None) -> DataFrame:
        """
        returns topic arns 
        Args:
            params (Dict): topic name
        """
        response = self.connection.list_topics()
        json_response = str(response)
        if params is not None and 'name' in params:
            name = params["name"]
            for topic_arn_row in response['Topics']:
                topic_arn_name = topic_arn_row['TopicArn']
                if name in topic_arn_name:
                    return [{'TopicArn': topic_arn_name}]
        if params is not None and 'name' in params:
            return []
        json_response = json_response.replace("\'", "\"")
        data = JSON.loads(str(json_response))
        return DataFrame(data["Topics"])


    def publish_message(self, params: Dict = None) -> DataFrame:
        """
        get topic_arn and message from params and sends message to amazon topic
        Args:
           params (Dict): topic name
        """
        json = self.connection.publish(TopicArn=params['topic_arn'], Message=params['message'])
        print(json)
        return DataFrame(json)


    def publish_batch(self, params: Dict = None) -> DataFrame:
        """
        get topic_arn and 
        publish multiple messages in a single batch (see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish_batch.html)
        """
        json = self.connection.publish_batch(TopicArn=params['topic_arn'],
                                      PublishBatchRequestEntries=params['batch_request_entries'])
        return DataFrame(json['Successful'])
        


    def create_topic(self, params: Dict = None) -> DataFrame:
        """
        create topic arguments topic name
        """
        name = params["name"]
        json = self.connection.create_topic(Name=name)
        return DataFrame(json)

    def call_sns_api(self, method_name: str = None, params: dict = None) -> DataFrame:
        """Calls the sns API method with the given params.

        """
        if method_name == 'create_topic':
            return self.create_topic(params)
        elif method_name == 'topic_list':
            return self.topic_list(params)
        elif method_name == 'publish_message':
            return self.publish_message(params)
        elif method_name == 'publish_batch':
            return self.publish_batch(params)
        else:
            raise NotImplementedError(f'Unknown method {method_name}')


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
