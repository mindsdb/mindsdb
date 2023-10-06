import os
import pytz
import io
import requests

import firebase_admin
from firebase_admin import credentials, messaging, exceptions


from mindsdb.integrations.api_handler import APIHandler, HandlerStatusResponse, HandlerResponse
from pandas import DataFrame

from mindsdb.utilities import log
from mindsdb.utilities.config import Config

from mindsdb.integrations.libs.api_handler import APIHandler

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)


class FirebaseHandler(APIHandler):
    """
        Firebase Habdler Implementationj
    """

    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        args = kwargs.get('connection_data', {})

        self.connection_args = {}

        handler_config = Config().get('firebase_handler', {})
        for k in ['firebase.json', 'url']:
            if k in args:
                self.connection_args[k] = args[k]
            elif f'FIREBASE_{k.upper()}' in os.environ:
                self.connection_args[k] = os.environ[f'FIREBASE_{k.upper()}']
            elif k in handler_config:
                self.connection_args[k] = handler_config[k]

        self.api = None
        self.is_connected = False

    def create_connection(self):

        # contains the service key in the path
        firebase_cred  = credentials.Certificate(self.connection_args['firebase.json'])
        self.connection = firebase_admin.initialize_app(self.firebase_cred)

        return self.connection


    def connect(self):
        """
            Authenticate with the Firebase API using the API keys and secrets
        """

        if self.is_connected is True:
            return self.api

        self.api = self.create_connection()

        self.is_connected = True
        return self.api

    def check_connection(self) -> StatusResponse:
        response = StatusResponse(False)

        try:
            api = self.connect()

            app_options = api.get_app().options
            response.success = True

        except exceptions.FirebaseError as e:
            response.error_message = f'Error connecting to Firebase api: {e}. Check bearer_token'
            log.logger.error(response.error_message)

        self.is_connected = response.success

        return response
        if not filters:
            return data

        data2 = []
        for row in data:
            add = False
            for op, key, value in filters:
                value2 = row.get(key)
                if isinstance(value, int):
                    # firebase returns ids as string
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


    def send_push_notification(self,title, message, registrationTokens):
        if len(registrationTokens) > 1:
            message = messaging.MulticastMessage(
                notification = messaging.Notification(
                    title = title
                    body = message
                )
                tokens = registrationTokens
            )
            response = messaging.send(message)
            print(response)
            
        else if len(registrationTokens) == 1:
            message = messaging.Message(
                notification = messaging
                .Notification(
                    title = title
                    body = message
                )
                token = registrationTokens[0]
            )
            response = messaging.send_multicast(message)
            print(response.success_count)

        
