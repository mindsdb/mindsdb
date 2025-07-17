import os

import re
from twilio.rest import Client
import pandas as pd
from mindsdb.integrations.libs.api_handler import APIHandler, APITable
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.utilities.config import Config
from mindsdb.utilities import log
from mindsdb.integrations.utilities.date_utils import parse_local_date
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions, project_dataframe, filter_dataframe

from mindsdb_sql_parser import ast

logger = log.getLogger(__name__)


class PhoneNumbersTable(APITable):

    def select(self, query: ast.Select) -> Response:

        conditions = extract_comparison_conditions(query.where)

        params = {}
        filters = []
        for op, arg1, arg2 in conditions:

            if op == 'or':
                raise NotImplementedError('OR is not supported')
            else:
                filters.append([op, arg1, arg2])

        if query.limit is not None:
            params['limit'] = query.limit.value

        result = self.handler.list_phone_numbers(params, df=True)

        # filter targets
        result = filter_dataframe(result, filters)

        # project targets
        result = project_dataframe(result, query.targets, self.get_columns())

        return result

    def get_columns(self):
        return [
            'sid',
            'date_created',
            'date_updated',
            'phone_number',
            'friendly_name',
            'account_sid',
            'capabilities',
            'number_status',
            'api_version',
            'voice_url',
            'sms_url',
            'uri'
        ]


class MessagesTable(APITable):

    def select(self, query: ast.Select) -> Response:

        conditions = extract_comparison_conditions(query.where)

        params = {}
        filters = []
        for op, arg1, arg2 in conditions:

            if op == 'or':
                raise NotImplementedError('OR is not supported')
            if arg1 == 'sent_at' and arg2 is not None:

                date = parse_local_date(arg2)

                if op == '>':
                    params['date_sent_after'] = date
                elif op == '<':
                    params['date_sent_before'] = date
                else:
                    raise NotImplementedError

                # also add to post query filter because date_sent_after=date1 will include date1
                filters.append([op, arg1, arg2])

            elif arg1 == 'sid':
                if op == '=':
                    params['sid'] = arg2
                # TODO: implement IN
                else:
                    NotImplementedError('Only  "from_number=" is implemented')
            elif arg1 == 'from_number':
                if op == '=':
                    params['from_number'] = arg2
                # TODO: implement IN
                else:
                    NotImplementedError('Only  "from_number=" is implemented')

            elif arg1 == 'to_number':
                if op == '=':
                    params['to_number'] = arg2
                # TODO: implement IN
                else:
                    NotImplementedError('Only  "to_number=" is implemented')

            else:
                filters.append([op, arg1, arg2])

        result = self.handler.fetch_messages(params, df=True)

        # filter targets
        result = filter_dataframe(result, filters)

        if query.limit is not None:
            result = result[:int(query.limit.value)]

        # project targets
        result = project_dataframe(result, query.targets, self.get_columns())

        return result

    def get_columns(self):
        return [
            'sid',
            'from_number',
            'to_number',
            'body',
            'direction',
            'msg_status',
            'sent_at',  # datetime.strptime(str(msg.date_sent), '%Y-%m-%d %H:%M:%S%z'),
            'account_sid',
            'price',
            'price_unit',
            'api_version',
            'uri'
        ]

    def insert(self, query: ast.Insert):
        # https://docs.tweepy.org/en/stable/client.html#tweepy.Client.create_tweet
        columns = [col.name for col in query.columns]

        ret = []

        insert_params = ["to_number", "from_number", "body", 'media_url']
        for row in query.values:
            params = dict(zip(columns, row))

            # split long text over 1500 symbols
            max_text_len = 1500
            text = params['body']
            words = re.split('( )', text)
            messages = []

            text2 = ''
            pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
            for word in words:
                # replace the links in word to string with the length as twitter short url (23)
                word2 = re.sub(pattern, '-' * 23, word)
                if len(text2) + len(word2) > max_text_len - 3 - 7:  # 3 is for ..., 7 is for (10/11)
                    messages.append(text2.strip())

                    text2 = ''
                text2 += word

            # the last message
            if text2.strip() != '':
                messages.append(text2.strip())

            len_messages = len(messages)

            for i, text in enumerate(messages):
                if i < len_messages - 1:
                    text += '...'
                else:
                    text += ' '

                if i >= 1:
                    text += f'({i + 1}/{len_messages})'
                    # only send image on first url
                    if 'media_url' in params:
                        del params['media_url']

                params['body'] = text
                params_to_send = {key: params[key] for key in insert_params if (key in params)}
                ret_row = self.handler.send_sms(params_to_send, ret_as_dict=True)
                ret_row['body'] = text
                ret.append(ret_row)

        return pd.DataFrame(ret)


class TwilioHandler(APIHandler):

    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        args = kwargs.get('connection_data', {})

        self.connection_args = {}
        handler_config = Config().get('twilio_handler', {})
        for k in ['account_sid', 'auth_token']:
            if k in args:
                self.connection_args[k] = args[k]
            elif f'TWILIO_{k.upper()}' in os.environ:
                self.connection_args[k] = os.environ[f'TWILIO_{k.upper()}']
            elif k in handler_config:
                self.connection_args[k] = handler_config[k]

        self.client = None
        self.is_connected = False

        messages = MessagesTable(self)
        phone_numbers = PhoneNumbersTable(self)
        self._register_table('messages', messages)
        self._register_table('phone_numbers', phone_numbers)

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
            logger.error(response.error_message)

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def parse_native_query(self, query_string: str):
        """Parses the native query string of format method(arg1=val1, arg2=val2, ...) and returns the method name and arguments."""

        # Adjust regex to account for the possibility of no arguments inside the parenthesis
        match = re.match(r'(\w+)\(([^)]*)\)', query_string)
        if not match:
            raise ValueError(f"Invalid query format: {query_string}")

        method_name = match.group(1)
        arg_string = match.group(2)

        # Extract individual arguments
        args = {}
        if arg_string:  # Check if there are any arguments
            for arg in arg_string.split(','):
                arg = arg.strip()
                key, value = arg.split('=')
                args[key.strip()] = value.strip()

        return method_name, args

    def native_query(self, query_string: str = None):
        '''It parses any native statement string and acts upon it (for example, raw syntax commands).'''

        method_name, params = self.parse_native_query(query_string)
        if method_name == 'send_sms':
            response = self.send_sms(params)
        elif method_name == 'fetch_messages':
            response = self.fetch_messages(params)
        elif method_name == 'list_phone_numbers':
            response = self.list_phone_numbers(params)
        else:
            raise ValueError(f"Method '{method_name}' not supported by TwilioHandler")

        return response

    def send_sms(self, params, ret_as_dict=False):
        message = self.client.messages.create(
            to=params.get("to_number"),
            from_=params.get('from_number'),
            body=params.get("body"),
            media_url=params.get("media_url")
        )

        if ret_as_dict is True:
            return {'sid': message.sid, 'status': message.status}
        return Response(
            RESPONSE_TYPE.MESSAGE,
            sid=message.sid,
            status=message.status
        )

    def fetch_messages(self, params, df=False):
        limit = int(params.get('limit', 1000))
        sid = params.get('sid', None)
        # Convert date strings to datetime objects if provided
        date_sent_after = params.get('date_sent_after', None)
        date_sent_before = params.get('date_sent_before', None)
        # Extract 'from_' and 'body' search criteria from params
        from_number = params.get('from_number', None)
        to_number = params.get('to_number', None)
        args = {
            'limit': limit,
            'date_sent_after': date_sent_after,
            'date_sent_before': date_sent_before,
            'from_': from_number,
            'to': to_number
        }

        args = {arg: val for arg, val in args.items() if val is not None}
        if sid:
            messages = [self.client.messages(sid).fetch()]
        else:
            messages = self.client.messages.list(**args)

        # Extract all possible properties for each message
        data = []
        for msg in messages:
            msg_data = {
                'sid': msg.sid,
                'to_number': msg.to,
                'from_number': msg.from_,
                'body': msg.body,
                'direction': msg.direction,
                'msg_status': msg.status,
                'sent_at': msg.date_created.replace(tzinfo=None),
                'account_sid': msg.account_sid,
                'price': msg.price,
                'price_unit': msg.price_unit,
                'api_version': msg.api_version,
                'uri': msg.uri,
                # 'media_url': [media.uri for media in msg.media.list()]
                # ... Add other properties as needed
            }
            data.append(msg_data)

        if df is True:
            return pd.DataFrame(data)
        return Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame(data))

    def list_phone_numbers(self, params, df=False):
        limit = int(params.get('limit', 100))
        args = {
            'limit': limit
        }
        args = {arg: val for arg, val in args.items() if val is not None}
        phone_numbers = self.client.incoming_phone_numbers.list(**args)

        # Extract properties for each phone number
        data = []
        for number in phone_numbers:
            num_data = {
                'sid': number.sid,
                'date_created': number.date_created,
                'date_updated': number.date_updated,
                'phone_number': number.phone_number,
                'friendly_name': number.friendly_name,
                'account_sid': number.account_sid,
                'capabilities': number.capabilities,
                'number_status': number.status,
                'api_version': number.api_version,
                'voice_url': number.voice_url,
                'sms_url': number.sms_url,
                'uri': number.uri,
                # ... Add other properties as needed
            }
            data.append(num_data)

        if df is True:
            return pd.DataFrame(data)
        return Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame(data))
