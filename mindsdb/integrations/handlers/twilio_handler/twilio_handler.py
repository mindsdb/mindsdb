import os

import re
from twilio.rest import Client
import pandas as pd
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.utilities.config import Config
from mindsdb.utilities import log


class TwilioHandler(APIHandler):

    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        args = kwargs.get('connection_data', {})

        self.connection_args = {}
        handler_config = Config().get('twilio_handler', {})
        for k in ['account_sid', 'auth_token', 'phone_number']:
            if k in args:
                self.connection_args[k] = args[k]
            elif f'TWILIO_{k.upper()}' in os.environ:
                self.connection_args[k] = os.environ[f'TWILIO_{k.upper()}']
            elif k in handler_config:
                self.connection_args[k] = handler_config[k]

        self.client = None
        self.is_connected = False

    def connect(self):
        """Authenticate with the Twilio API using the account_sid and auth_token provided in the constructor."""
        if self.is_connected is True:
            return self.client

        self.client = Client(
            self.connection_args['account_sid'],
            self.connection_args['auth_token']
        )

        self.is_connected = True
        return self.client

    def check_connection(self) -> StatusResponse:
        '''It evaluates if the connection with Twilio API is alive and healthy.'''
        response = StatusResponse(False)

        try:
            self.connect()
            # Maybe make a harmless API request to verify connection, but be mindful of rate limits and costs
            response.success = True

        except Exception as e:
            response.error_message = f'Error connecting to Twilio api: {e}. '
            log.logger.error(response.error_message)

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response


    def parse_native_query(self, query_string: str):
        """Parses the native query string of format method(arg1=val1, arg2=val2, ...) and returns the method name and arguments."""
        
        # Extract method name and argument string
        print(f"query_string {query_string}")
        match = re.match(r'(\w+)\(([^)]+)\)', query_string)
        if not match:
            raise ValueError(f"Invalid query format: {query_string}")

        method_name = match.group(1)
        arg_string = match.group(2)

        # Extract individual arguments
        args = {}
        for arg in arg_string.split(','):
            arg = arg.strip()
            key, value = arg.split('=')
            args[key.strip()] = value.strip()

        return method_name, args

    def native_query(self, query_string: str = None):
        '''It parses any native statement string and acts upon it (for example, raw syntax commands).'''

        method_name, params = self.parse_native_query(query_string)
        print(f"params {params}")
        if method_name == 'send_sms':
            response = self.send_sms(params)
        elif method_name == 'fetch_messages':
            response = self.fetch_messages(params)
        else:
            raise ValueError(f"Method '{method_name}' not supported by TwilioHandler")

        return response

    def send_sms(self, params):
        message = self.client.messages.create(
            to=params.get("to"),
            from_=self.connection_args['phone_number'],
            body=params.get("body")
        )
        return Response(
            RESPONSE_TYPE.MESSAGE,
            message_sid=message.sid,
            status=message.status
        )

    
    

    def fetch_messages(self, params):
        n = int(params.get('n', 10))

         # Convert date strings to datetime objects if provided
        date_sent_after = params.get('date_sent_after', None)
        if date_sent_after:
            date_sent_after = datetime.strptime(date_sent_after, '%Y-%m-%d')

        date_sent_before = params.get('date_sent_before', None)
        if date_sent_before:
            date_sent_before = datetime.strptime(date_sent_before, '%Y-%m-%d')

        # Extract 'from_' and 'body' search criteria from params
        from_ = params.get('from_', None)
        body = params.get('body', None)

        messages = self.client.messages.list(
            limit=n,
            date_sent_after=date_sent_after,
            date_sent_before=date_sent_before,
            from_=from_)

        # Extract all possible properties for each message
        data = []
        for msg in messages:
            msg_data = {
                'sid': msg.sid,
                'account_sid': msg.account_sid,
                'messaging_service_sid': msg.messaging_service_sid,
                'body': msg.body,
                'status': msg.status,
                'to': msg.to,
                'from_': msg.from_,
                'price': msg.price,
                'price_unit': msg.price_unit,
                'direction': msg.direction,
                'api_version': msg.api_version,
                'date_created': msg.date_created,
                'date_updated': msg.date_updated,
                'date_sent': msg.date_sent,
                'uri': msg.uri,
                # ... Add other properties as needed
            }
            data.append(msg_data)

        return Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame(data))
