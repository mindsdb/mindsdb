import pandas as pd

from solace.messaging.messaging_service import MessagingService, RetryStrategy
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.receiver.message_receiver import MessageHandler
from solace.messaging.resources.topic import Topic
from solace.messaging.receiver.inbound_message import InboundMessage

from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser import ast

from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class SolaceHandler(DatabaseHandler):

    def __init__(self, name: str = None, **kwargs):
        """Registers all API tables and prepares the handler for an API connection.

        Args:
            name: (str): The handler name to use
        """
        super().__init__(name)
        args = kwargs.get('connection_data', {})

        if 'host' not in args:
            raise ValueError('Host parameter is required')

        self.connection_args = args
        self.messaging_service = None
        self.is_connected = False

    def connect(self):

        broker_props = {
            "solace.messaging.transport.host": self.connection_args['host'],
            "solace.messaging.service.vpn-name": self.connection_args.get('vpn-name', 'default'),
            "solace.messaging.authentication.scheme.basic.username": self.connection_args.get('username'),
            "solace.messaging.authentication.scheme.basic.password": self.connection_args.get('password')
        }

        self.messaging_service = MessagingService\
            .builder()\
            .from_properties(broker_props) \
            .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20, 3)) \
            .build()

        self.messaging_service.connect()

        self.direct_publisher = self.messaging_service\
            .create_direct_message_publisher_builder()\
            .build()
        self.direct_publisher.start()

        # TODO support persistent_publisher ?

        self.is_connected = True
        return self.messaging_service

    def disconnect(self):

        if self.is_connected is False:
            return

        self.direct_publisher.terminate()
        self.messaging_service.disconnect()
        self.is_connected = False
        return self.is_connected

    def check_connection(self) -> StatusResponse:

        response = StatusResponse(False)

        try:
            self.connect()
            response.success = True
        except Exception as e:
            logger.error(f'Error connecting to Solace: {e}!')
            response.error_message = e

        if response.success is False:
            self.is_connected = False
        return response

    def native_query(self, query: str = None) -> Response:
        ast = parse_sql(query)
        return self.query(ast)

    def query(self, query: ast.ASTNode):
        if isinstance(query, ast.Insert):
            result = self.handle_insert(query)
        else:
            raise NotImplementedError
        return result

    def get_columns(self, table_name: str) -> Response:
        df = pd.DataFrame([], columns=['Field'])
        df['Type'] = 'str'

        return Response(RESPONSE_TYPE.TABLE, df)

    def get_tables(self) -> Response:
        df = pd.DataFrame([], columns=['table_name', 'table_type'])

        return Response(RESPONSE_TYPE.TABLE, df)

    def handle_insert(self, query: ast.Insert):

        message_builder = self.messaging_service.message_builder()

        topic_name = '/'.join(query.table.parts)

        column_names = [col.name for col in query.columns]
        for insert_row in query.values:
            data = dict(zip(column_names, insert_row))

            outbound_message = message_builder.build(data)
            self.direct_publisher.publish(destination=Topic.of(topic_name), message=outbound_message)

        return Response(RESPONSE_TYPE.OK)

    def subscribe(self, stop_event, callback, table_name, columns=None, **kwargs):

        class MessageHandlerImpl(MessageHandler):
            def on_message(self, message: 'InboundMessage'):
                # Check if the payload is a dict
                payload = message.get_payload_as_dictionary()
                if payload is not None:
                    data = payload
                else:
                    # check as string
                    payload = message.get_payload_as_string()
                    if payload is None:
                        payload = message.get_payload_as_bytes()
                        if isinstance(payload, bytearray):
                            payload = payload.decode()
                    data = {'data': payload}

                if columns is not None:
                    updated_columns = data.keys()
                    if not set(columns) & set(updated_columns):
                        # skip
                        return

                callback(data)

        table = ast.Identifier(table_name)
        topic_name = '/'.join(table.parts)

        topics = [TopicSubscription.of(topic_name)]
        direct_receiver = self.messaging_service\
            .create_direct_message_receiver_builder()\
            .with_subscriptions(topics)\
            .build()

        direct_receiver.start()
        direct_receiver.receive_async(MessageHandlerImpl())

        stop_event.wait()

        direct_receiver.terminate()
