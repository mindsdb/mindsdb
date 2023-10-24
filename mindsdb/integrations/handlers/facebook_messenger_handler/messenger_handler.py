# facebookmessenger_handler.py
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb_sql.parser import ast
from mindsdb.integrations.libs.api_handler import APIHandler, APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.integrations.utilities.date_utils import parse_utc_date

# Import additional required libraries
import requests
import json

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

        return result\


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


    def insert(self, query: ast.Insert):
        # Implement this method to send messages to Facebook Messenger
        pass

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

    def connect(self):
        # Implement this method to authenticate with the Facebook Messenger API
        pass

    def check_connection(self) -> StatusResponse:
        # Implement this method to check the connection to the Facebook Messenger API
        pass

    def native_query(self, query_string: str = None):
        # Implement this method to execute a native query on the Facebook Messenger API
        pass
