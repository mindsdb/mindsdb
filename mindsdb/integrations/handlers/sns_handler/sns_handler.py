from collections import OrderedDict
from typing import Dict
from typing import Optional

import boto3
from mindsdb_sql import parse_sql
from pandas import DataFrame

from mindsdb.integrations.handlers.sns_handler.sns_tables import MessageTable
from mindsdb.integrations.handlers.sns_handler.sns_tables import SubscriptionTable
from mindsdb.integrations.handlers.sns_handler.sns_tables import TopicTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.libs.response import (

    HandlerStatusResponse as StatusResponse

)
from mindsdb.utilities import log


class SnsHandler(APIHandler):
    """

    This handler handles connection and execution of the sns statements.

    """

    name = 'sns'

    connection = None

    sqs_client = None

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

            TopicTable, MessageTable, SubscriptionTable

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

            endpoint_url=self.connection_data['endpoint_url'],

            region_name=self.connection_data['region_name'])

        self.sqs_client = boto3.client('sqs',

                                       aws_access_key_id=self.connection_data['aws_access_key_id'],

                                       aws_secret_access_key=self.connection_data['aws_secret_access_key'],

                                       endpoint_url=self.connection_data['endpoint_url'],

                                       region_name=self.connection_data['region_name']

                                       )

        self.is_connected = True

        return self.connection

    def publish_message(self, params: Dict = None) -> DataFrame:
        """

        get topic_arn and message from params and sends message to amazon topic

        Returns results as a pandas DataFrame.

        Args:

           params (Dict): topic name (str) and message (str)

        """

        response = self.connection.publish(TopicArn=params['topic_arn'], Message=params['message'])

        return DataFrame(response)

    def publish_batch(self, params: Dict = None) -> DataFrame:
        """

        get topic_arn and

        publish multiple messages in a single batch (see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish_batch.html)

        Returns results as a pandas DataFrame.

        Args:

            params (Dict):  topic_arn (str) and  batch_request_entries(dict)

        """

        response = self.connection.publish_batch(TopicArn=params['topic_arn'],

                                                 PublishBatchRequestEntries=params['batch_request_entries'])

        return DataFrame(response['Successful'])

    def create_topic(self, name: None) -> DataFrame:
        """

        create topic

        Args:

           params (name):

        Returns results as a pandas DataFrame.

        """

        response = self.connection.create_topic(Name=name)

        return DataFrame(response)

    def delete_topic(self, topic_arn: None) -> DataFrame:
        """

        delete topic by TopicArn

        Args:

           params (Dict):

        Returns results as a pandas DataFrame.

        """

        response = self.connection.delete_topic(TopicArn=topic_arn)

        return DataFrame(response)


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
