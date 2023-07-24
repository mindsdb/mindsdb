import email_helpers
from collections import defaultdict

import pandas as pd

from mindsdb.utilities import log

from mindsdb.integrations.libs.api_handler import APIHandler, FuncParser

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

from email_tables import EmailsTable


class EmailHandler(APIHandler):
    """A class for handling connections and interactions with Email (send and search).

    Attributes:
        "email": "Email address used to login to the SMTP and IMAP servers.",
        "password": "Password used to login to the SMTP and IMAP servers.",
        "smtp_server": "SMTP server to be used for sending emails. Default value is 'smtp.gmail.com'.",
        "smtp_port": "Port number for the SMTP server. Default value is 587.",
        "imap_server": "IMAP server to be used for reading emails. Default value is 'imap.gmail.com'."
    """

    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        args = kwargs.get('connection_data', {})

        self.connection_args = {}
        for k in ['email', 'smtp_server', 'smtp_port', 'imap_server',
                  'username', 'password']:
            if k in args:
                self.connection_args[k] = args[k]

        self.api = None
        self.is_connected = False

        emails = EmailsTable(self)
        self._register_table('emails', emails)

    def connect(self):
        """Authenticate with the email servers using credentials."""

        if self.is_connected is True:
            return self.api

        try:
            self.api = email_helpers.EmailClient(**self.connection_args)
        except Exception as e:
            log.logger.error(f'Error connecting to email api: {e}!')
            raise e

        self.is_connected = True
        return self.api

    def check_connection(self) -> StatusResponse:

        response = StatusResponse(False)

        try:
            api = self.connect()
            api.get_user(id=1)

            response.success = True

        except Exception as e:
            log.logger.error(f'Error connecting to email api: {e}!')
            response.error_message = e

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query_string: str = None):
        method_name, params = FuncParser().from_string(query_string)

        df = self.call_email_api(method_name, params)

        return Response(
            RESPONSE_TYPE.TABLE,
            data_frame=df
        )

    def call_email_api(self, method_name:str = None, params:dict = None):

        # method > table > columns
        expansions_map = {
            'search_recent_tweets': {
                'users': ['author_id', 'in_reply_to_user_id'],
            },
            'search_all_tweets': {
                'users': ['author_id'],
            },
        }

        self.connect()
        method = getattr(self.api.imap_server, method_name)

        # pagination handle

        includes = defaultdict(list)

        resp, items = resp = method(**params)

        df = pd.DataFrame(items)

        return df

