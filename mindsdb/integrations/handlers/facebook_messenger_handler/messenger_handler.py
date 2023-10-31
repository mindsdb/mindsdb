import re
import os
import datetime as dt
import ast
import time
from collections import defaultdict
import pytz
import io
from fbmessenger import MessengerClient
from fbmessenger.elements import Text
from fbmessenger.quick_replies import QuickReply, QuickReplies
from fbmessenger.buttons import URLButton, PostbackButton
from fbmessenger.threads import ThreadType
import pandas as pd
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb_sql.parser import ast
from mindsdb.integrations.libs.api_handler import APIHandler, APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.integrations.utilities.date_utils import parse_utc_date
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

# Import additional required libraries
import requests
import json

from messengerapi import SendApi
from messengerapi.components import Elements, Element, Buttons, Button, QuickReply


class MessagesTable(APITable):
    def select(self, query: ast.Select) -> Response:
        conditions = extract_comparison_conditions(query.where)
        params = {}
        filters = []

        for op, arg1, arg2 in conditions:
            if op == 'text':
                if arg1 == 'message':
                    client = Messager('<access_token>')
                    client.send_text(arg2, 'Recipient ID')
            elif op == 'image':
                if arg1 == 'message':
                    send_api = SendApi('<page_access_token>')
                    send_api.send_local_image(arg2 , 'Recipient ID')
            elif op == 'audio':
                if arg1 == 'message':
                    send_api = SendApi('<page_access_token>')
                    send_api.send_local_audio(arg2 , 'Recipient ID')
            elif op == 'video':
                if arg1 == 'message':
                    send_api = SendApi('<page_access_token>')
                    send_api.send_local_video(arg2 , 'Recipient ID')
            elif op == 'file':
                if arg1 == 'message':
                    send_api = SendApi('<page_access_token>')
                    send_api.send_local_file(arg2 , 'Recipient ID')
            elif op == 'quick_reply':
                if arg1 == 'message':
                    client = Messager('<access_token>')
                    client.send_quick_replies('Recipient ID', arg2, [
                        QuickReply("Option 1", "PAYLOAD_1"),
                        QuickReply("Option 2", "PAYLOAD_2"),
                        QuickReply("Option 3", "PAYLOAD_3")
                    ])
            elif op == 'generic_template':
                if arg1 == 'message':
                    send_api = SendApi('<page_access_token>')
                    elements = Elements()
                    buttons = Buttons()
                    button = Button(button_type=POSTBACK, title="My button")
                    buttons.add_button(button.get_content())
                    element = Element(title="My element", subtitle="The element's subtitle", image_url=arg2, buttons=buttons)
                    elements.add_element(element.get_content())
                    send_api.send_generic_message(elements.get_content() , 'Recipient ID' , image_aspect_ratio="horizontal")

        if query.limit is not None:
            params['limit'] = query.limit.value

        if 'thread_id' not in params:
            # Default to general thread if not specified
            params['thread_id'] = 'general'

        result = self.handler.call_messenger_api(
            method_name='get_thread_messages',
            params=params,
            filters=filters
        )

        # Filter targets
        columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                columns = []
                break
            elif isinstance(target, ast.Identifier):
                columns.append(target.parts[-1])
            else:
                raise NotImplementedError

        if len(columns) == 0:
            columns = self.get_columns()

        # Convert columns to lower case
        columns = [name.lower() for name in columns]

        if len(result) == 0:
            result = pd.DataFrame([], columns=columns)
        else:
            # Add absent columns
            for col in set(columns) & set(result.columns) ^ set(columns):
                result[col] = None

            # Filter by columns
            result = result[columns]

        return result


    def get_columns(self):
        return [
            'id',
            'created_time',
            'message',
            'from_id',
            'from_name',
            'to_id',
            'to_name',
        ]


def insert(self, query):
    columns = [col.name for col in query.columns]

    insert_params = ('page_access_token', 'recipient_id')
    for p in insert_params:
        if p not in self.handler.connection_args:
            raise Exception(f'To insert data into Facebook Messenger, you need to provide the following parameters when connecting it to MindsDB: {insert_params}')

    for row in query.values:
        params = dict(zip(columns, row))

        # Prepare the request
        headers = {
            'Content-Type': 'application/json'
        }
        payload = {
            'recipient': {
                'id': self.handler.connection_args['recipient_id']
            },
            'message': params['message']
        }

        # Send the request
        response = requests.post('https://graph.facebook.com/v13.0/me/messages?access_token=' + self.handler.connection_args['page_access_token'], headers=headers, json=payload)

        # Check the response
        if response.status_code != 200:
            raise Exception('Failed to send message: ' + response.text)

    def update_persistent_menu(self, menu):
        headers = {
            'Content-Type': 'application/json'
        }
        payload = {
            'persistent_menu': [
                {
                    'locale': 'default',
                    'composer_input_disabled': False,
                    'call_to_actions': menu
                }
            ]
        }
        response = requests.post('https://graph.facebook.com/v13.0/me/messenger_profile?access_token=' + self.page_access_token, headers=headers, json=payload)
        if response.status_code != 200:
            raise Exception('Failed to update persistent menu: ' + response.text)


class FacebookMessengerHandler(APIHandler):
    def __init__(self, name=None, **kwargs):
        super().__init__(name)
        args = kwargs.get('connection_data', {})
        self.connection_args = {}
        handler_config = Config().get('facebookmessenger_handler', {})
        for k in ['access_token']:
            if k in args:
                self.connection_args[k] = args[k]
            elif f'FACEBOOK_{k.upper()}' in os.environ:
                self.connection_args[k] = os.environ[f'FACEBOOK_{k.upper()}']
            elif k in handler_config:
                self.connection_args[k] = handler_config[k]
        self.is_connected = False
        messages = MessagesTable(self)
        self._register_table('messages', messages)

    def connect(self, api_version=2):
        """Authenticate with the Facebook Messenger API using the API keys and secrets stored in the `consumer_key`, `consumer_secret`, `access_token`, and `access_token_secret` attributes."""  # noqa

        if self.is_connected is True:
            return self.messenger_api

        self.messenger_api = self.create_connection()

        self.is_connected = True
        return self.messenger_api


    def check_connection(self) -> StatusResponse:

        response = StatusResponse(False)

        try:
            api = self.connect()

            # call get_user_profile with unknown id.
            #   it raises an error in case if auth is not success and returns not-found otherwise
            api.get_user_profile(user_id=1)
            response.success = True

        except fbmessenger.Unauthorized as e:
            response.error_message = f'Error connecting to Facebook Messenger api: {e}. Check bearer_token'
            log.logger.error(response.error_message)

        if response.success is True and len(self.connection_args) > 1:
            # not only bearer_token, check read-write mode (OAuth 2.0 Authorization Code with PKCE)
            try:
                api = self.connect()

                api.get_my_profile()

            except fbmessenger.Unauthorized as e:
                keys = 'consumer_key', 'consumer_secret', 'access_token', 'access_token_secret'
                response.error_message = f'Error connecting to Facebook Messenger api: {e}. Check' + ', '.join(keys)
                log.logger.error(response.error_message)

                response.success = False

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response


    def native_query(self, query_string: str = None):
        method_name, params = FuncParser().from_string(query_string)

        df = self.call_facebook_messenger_api(method_name, params)

        return Response(
            RESPONSE_TYPE.TABLE,
            data_frame=df
        )

    def _apply_filters(self, data, filters):
        if not filters:
            return data
        data2 = []
        for row in data:
            add = False
            for op, key, value in filters:
                value2 = row.get(key)
                if isinstance(value, int):
                    # Facebook Messenger returns ids as string
                    value = str(value)

                if op in ('!=', '<>'):
                    if value == value2:
                        break
                elif op in ('==', '='):
                    if value != value2:
                        break
                elif op == 'in':
                    if not isinstance(value, list):
                        value = [value]
                    if value2 not in value:
                        break
                elif op == 'not in':
                    if not isinstance(value, list):
                        value = [value]
                    if value2 in value:
                        break
                else:
                    raise NotImplementedError(f'Unknown filter: {op}')
                # only if there wasn't breaks
                add = True
            if add:
                data2.append(row)
        return data2

    def call_facebook_messenger_api(self, method_name: str = None, params: dict = None, filters: list = None):
        # method > table > columns
        expansions_map = {
            'user': {
                'table': 'users',
                'columns': {
                    'id': 'id',
                    'name': 'name',
                    'first_name': 'first_name',
                    'last_name': 'last_name',
                    'profile_pic': 'profile_pic',
                    'locale': 'locale',
                    'timezone': 'timezone',
                    'gender': 'gender',
                    'is_payment_enabled': 'is_payment_enabled',
                    'last_ad_referral': 'last_ad_referral',
                    'search_recent_messages': {
                        'users': ['sender_id', 'recipient_id'],
                    },
                    'search_all_messages': {
                        'users': ['sender_id'],
                    },
                }.get(params.get('expansions', {}).get('user', {}), {})
            },
            'page': {
                'table': 'pages',
                'columns': {
                    'id': 'id',
                    'name': 'name',
                    'access_token': 'access_token',
                    'category': 'category',
                    'category_list': 'category_list',
                    'search_recent_messages': {
                        'users': ['sender_id', 'recipient_id'],
                    },
                    'search_all_messages': {
                        'users': ['sender_id'],
                    },
                }.get(params.get('expansions', {}).get('page', {}), {})
            },
            'message': {
                'table': 'messages',
                'columns': {
                    'id': 'id',
                    'text': 'text',
                    'attachments': 'attachments',
                    'quick_reply': 'quick_reply',
                    'is_echo': 'is_echo',
                    'app_id': 'app_id',
                    'metadata': 'metadata',
                    'platform': 'platform',
                    'created_at': 'created_at',
                    'search_recent_messages': {
                        'users': ['sender_id', 'recipient_id'],
                    },
                    'search_all_messages': {
                        'users': ['sender_id'],
                    },
                }.get(params.get('expansions', {}).get('message', {}), {})
            },
        }.get(params.get('expansions', {}).get('type', {}), {})

        api = self.connect()
        method = getattr(api, method_name) if method_name in dir(MessengerClient) else None

        # pagination handle

        count_results = None
        if 'max_results' in params:
            count_results = params['max_results']

        data = []
        includes = defaultdict(list)

        max_page_size = 100
        min_page_size = 10
        left = None

        limit_exec_time = time.time() + 60

        if filters:
            # if we have filters: do big page requests
            params['max_results'] = max_page_size

        while True:
            if time.time() > limit_exec_time:
                raise RuntimeError('Handler request timeout error')

            if count_results is not None:
                left = count_results - len(data)
                if left == 0:
                    break
                elif left < 0:
                    # got more results that we need
                    data = data[:left]
                    break

                if left > max_page_size:
                    params['max_results'] = max_page_size
                elif left < min_page_size:
                    params['max_results'] = min_page_size
                else:
                    params['max_results'] = left

            log.logger.debug(f'>>>facebook messenger in: {method_name}({params})')
            resp = method(**params) if method else None

            # handle response

            if resp and isinstance(resp, list):
                chunk = resp
            else:
                break

            if filters:
                chunk = self._apply_filters(chunk, filters)

            # limit output
            if left is not None:
                chunk = chunk[:left]

            data.extend(chunk)
            # next page ?
            if count_results is not None and 'next' in resp:
                params['after'] = resp['next']
            else:
                break

        df = pd.DataFrame(data)

        # enrich

        expansions = expansions_map.get(method_name)
        if expansions is not None:
            for table, records in includes.items():
                df_ref = pd.DataFrame(records)

                if table not in expansions:
                    continue

                for col_id in expansions[table]:
                    col = col_id[:-3]  # cut _id
                    if col_id not in df.columns:
                        continue

                    col_map = {
                        col_ref: f'{col}_{col_ref}'
                        for col_ref in df_ref.columns
                    }
                    df_ref2 = df_ref.rename(columns=col_map)
                    df_ref2 = df_ref2.drop_duplicates(col_id)

                    df = df.merge(df_ref2, on=col_id, how='left')

        return df