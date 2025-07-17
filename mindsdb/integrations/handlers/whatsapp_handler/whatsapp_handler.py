import os
from twilio.rest import Client
import re
from datetime import datetime as datetime
from typing import List
import pandas as pd

from mindsdb.utilities import log
from mindsdb.utilities.config import Config

from mindsdb_sql_parser import ast
from mindsdb.integrations.utilities.date_utils import parse_local_date

from mindsdb.integrations.libs.api_handler import APIHandler, APITable

from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions, project_dataframe, filter_dataframe

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

logger = log.getLogger(__name__)


class WhatsAppMessagesTable(APITable):
    def select(self, query: ast.Select) -> Response:
        """
        Retrieves messages sent/received from the database using Twilio Whatsapp API
        Returns
            Response: conversation_history
        """

        # Extract comparison conditions from the query
        conditions = extract_comparison_conditions(query.where)
        params = {}
        filters = []

        # Build the filters and parameters for the query
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
                else:
                    NotImplementedError('Only  "from_number=" is implemented')

            elif arg1 == 'from_number':
                if op == '=':
                    params['from_number'] = arg2
                else:
                    NotImplementedError('Only  "from_number=" is implemented')

            elif arg1 == 'to_number':
                if op == '=':
                    params['to_number'] = arg2
                else:
                    NotImplementedError('Only  "to_number=" is implemented')

            else:
                filters.append([op, arg1, arg2])

        # Fetch messages based on the filters
        result = self.handler.fetch_messages(params, df=True)

        # filter targets
        result = filter_dataframe(result, filters)

        # If limit is specified
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
        """
        Sends a whatsapp message

        Args:
            body: message body
            from_number: number from which to send the message
            to_number: number to which message will be sent
        """

        # get column names and values from the query
        columns = [col.name for col in query.columns]

        ret = []

        insert_params = ["body", "from_number", "to_number"]
        for row in query.values:
            params = dict(zip(columns, row))

            # Check text length
            max_text_len = 1500
            text = params["body"]
            words = re.split('( )', text)
            messages = []

            """
                Regex for matching if any URls are present, if yes then replace with string of hyphens(-)

                Example:
                    words = ['Check', ' ', 'out', ' ', 'this', ' ', 'cool', ' ', 'website:', ' ', 'https://example.com.', "It's", ' ', 'awesome!']

                    After parsing through regex ('https://example.com') URL is matched

                    Final output:
                    messages = ['Check - out - this - cool - website: ----------------------- "It\'s - awesome!']
            """

            text2 = ''
            pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
            for word in words:
                # replace the links in word to string with the length as twitter short url (23)
                word2 = re.sub(pattern, '-' * 23, word)
                if len(text2) + len(word2) > max_text_len - 3 - 7:  # 3 is for ..., 7 is for (10/11)
                    messages.append(text2.strip())

                    text2 = ''
                text2 += word

            # Parse last message
            if text2.strip() != '':
                messages.append(text2.strip())

            len_messages = len(messages)

            # Modify message based on the length
            for i, text in enumerate(messages):
                if i < len_messages - 1:
                    text += '...'
                else:
                    text += ' '

                if i >= 1:
                    text += f'({i + 1}/{len_messages})'

                # Pass parameters and call 'send_message'
                params['body'] = text
                params_to_send = {key: params[key] for key in insert_params if (key in params)}
                ret_row = self.handler.send_message(params_to_send, ret_as_dict=True)

                # Save the results
                ret_row['body'] = text
                ret.append(ret_row)

        return pd.DataFrame(ret)


class WhatsAppHandler(APIHandler):
    """
    A class for handling connections and interactions with Twilio WhatsApp API.
    Args:
        account_sid(str): Accound ID of the twilio account.
        auth_token(str): Authentication Token obtained from the twilio account.
    """

    def __init__(self, name=None, **kwargs):
        """
        Initializes the connection by checking all the params are provided by the user.
        """
        super().__init__(name)

        args = kwargs.get('connection_data', {})
        self.connection_args = {}
        handler_config = Config().get('whatsapp_handler', {})
        for k in ['account_sid', 'auth_token']:
            if k in args:
                self.connection_args[k] = args[k]
            elif f'TWILIO_{k.upper()}' in os.environ:
                self.connection_args[k] = os.environ[f'TWILIO_{k.upper()}']
            elif k in handler_config:
                self.connection_args[k] = handler_config[k]
        self.client = None
        self.is_connected = False

        messages = WhatsAppMessagesTable(self)
        self._register_table('messages', messages)

    def connect(self):
        """
        Authenticate with the Twilio API using the provided `account_SID` and `auth_token`.
        """
        if self.is_connected is True:
            return self.client

        self.client = Client(
            self.connection_args['account_sid'],
            self.connection_args['auth_token']
        )

        self.is_connected = True
        return self.client

    def check_connection(self) -> StatusResponse:
        """
        Checks the connection by performing a basic operation with the Twilio API.
        """
        response = StatusResponse(False)

        try:
            self.connect()
            response.success = True

        except Exception as e:
            response.error_message = f'Error connecting to Twilio API: {str(e)}. Check credentials.'
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
        """
        Retreievs the native query from the `parse_native_query` and calls appropriate function and returns the result of the query as a Response object.
        """
        method_name, params = self.parse_native_query(query_string)
        if method_name == 'send_message':
            response = self.send_message(params)
        else:
            raise ValueError(f"Method '{method_name}' not supported by TwilioHandler")

        return response

    def fetch_messages(self, params, df=False):
        """
        Gets conversation history

        Returns:
            Response: conversation history
        """
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

        # Create a DataFrame
            result_df = pd.DataFrame(data)

            # Filter rows where 'from_number' or 'to_number' begins with 'whatsapp:'
            result_df = result_df[result_df['from_number'].str.startswith('whatsapp:') | result_df['to_number'].str.startswith('whatsapp:')]

        if df is True:
            return pd.DataFrame(result_df)
        return Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame(result_df))

    def send_message(self, params, ret_as_dict=False) -> Response:
        """
        Sends a message to the given Whatsapp number.

        Args:
            body: message body
            from_number: number from which to send the message
            to_number: number to which message will be sent
        """
        try:
            message = self.client.messages.create(
                body=params.get('body'),
                to=params.get('to_number'),
                from_=params.get('from_number')
            )

            if ret_as_dict is True:
                return {"sid": message.sid, "from": message.from_, "to": message.to, "message": message.body, "status": message.status}

            return Response(
                RESPONSE_TYPE.MESSAGE,
                sid=message.sid,
                from_=message.from_,
                to=message.to,
                body=message.body,
                status=message.status
            )

        except Exception as e:
            # Log the exception for debugging purposes
            logger.error(f"Error sending message: {str(e)}")
            logger.exception(f"Error sending message: {str(e)}")
            raise Exception("Error sending message")

    def call_whatsapp_api(self, method_name: str = None, params: dict = None):
        """
        Calls specific method specified.

        Args:
            method_name: to call specific method
            params: parameters to call the method

        Returns:
            List of dictionaries as a result of the method call
        """
        api = self.connect()
        method = getattr(api, method_name)

        try:
            result = method(**params)
        except Exception as e:
            error = f"Error calling method '{method_name}' with params '{params}': {e}"
            logger.error(error)
            raise e

        if 'messages' in result:
            result['messages'] = self.convert_channel_data(result['messages'])

        return [result]

    def convert_channel_data(self, messages: List[dict]):
        """
        Convert the list of channel dictionaries to a format that can be easily used in the data pipeline.

        Args:
            channels: A list of channel dictionaries.

        Returns:
            A list of channel dictionaries with modified keys and values.
        """
        new_messages = []
        for message in messages:
            new_message = {
                'id': message['id'],
                'name': message['name'],
                'created': datetime.fromtimestamp(float(message['created']))
            }
            new_messages.append(new_message)
        return new_messages
