from mindsdb_sql_parser import parse_sql
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.utilities import log
from mindsdb.integrations.handlers.google_analytics_handler.google_analytics_tables import ConversionEventsTable
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)

import json
import os

from google.analytics.admin_v1beta import AnalyticsAdminServiceClient
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials as OAuth2Credentials
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError

DEFAULT_SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
                  'https://www.googleapis.com/auth/analytics.edit',
                  'https://www.googleapis.com/auth/analytics'
                  ]

logger = log.getLogger(__name__)


class GoogleAnalyticsHandler(APIHandler):
    """A class for handling connections and interactions with the Google Analytics Admin API.

    Attributes:
        credentials_file (str): The path to the Google Auth Credentials file for authentication
        and interacting with the Google Analytics API on behalf of the user.

        Scopes (List[str], Optional): The scopes to use when authenticating with the Google Analytics API.
    """

    name = 'google_analytics'

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.page_size = 500
        self.connection_args = kwargs.get('connection_data', {})
        self.property_id = self.connection_args['property_id']
        if self.connection_args.get('credentials'):
            self.credentials_file = self.connection_args.pop('credentials')

        self.scopes = self.connection_args.get('scopes', DEFAULT_SCOPES)
        self.service = None
        self.is_connected = False
        conversion_events = ConversionEventsTable(self)
        self.conversion_events = conversion_events
        self._register_table('conversion_events', conversion_events)

    def _get_creds_json(self):
        if 'credentials_file' in self.connection_args:
            if os.path.isfile(self.connection_args['credentials_file']) is False:
                raise Exception("credentials_file must be a file path")
            with open(self.connection_args['credentials_file']) as source:
                info = json.load(source)
            return info
        elif 'credentials_json' in self.connection_args:
            info = self.connection_args['credentials_json']
            if not isinstance(info, dict):
                raise Exception("credentials_json has to be dict")
            info['private_key'] = info['private_key'].replace('\\n', '\n')
            return info
        else:
            raise Exception('Connection args have to content ether credentials_file or credentials_json')

    def create_connection(self):
        if 'access_token' in self.connection_args:
            token = self.connection_args.get('access_token')
            if not token or not isinstance(token, str):
                raise Exception("access_token must be a non-empty string")

            creds = OAuth2Credentials(token, scopes=self.scopes)
            refresh_token = self.connection_args.get('refresh_token')
            client_id = self.connection_args.get('client_id')
            client_secret = self.connection_args.get('client_secret')
            token_uri = self.connection_args.get('token_uri')
            if refresh_token or client_id or client_secret or token_uri:
                creds = OAuth2Credentials(
                    token,
                    refresh_token=refresh_token,
                    token_uri=token_uri or "https://oauth2.googleapis.com/token",
                    client_id=client_id,
                    client_secret=client_secret,
                    scopes=self.scopes,
                )

            if not creds or not creds.valid:
                if creds and creds.expired and getattr(creds, 'refresh_token', None):
                    creds.refresh(Request())

            return AnalyticsAdminServiceClient(credentials=creds)

        info = self._get_creds_json()
        creds = service_account.Credentials.from_service_account_info(info=info, scopes=self.scopes)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())

        return AnalyticsAdminServiceClient(credentials=creds)

    def connect(self):
        """
        Authenticate with the Google Analytics Admin API using the credential file.

        Returns
        -------
        service: object
            The authenticated Google Analytics Admin API service object.
        """
        if self.is_connected is True:
            return self.service

        self.service = self.create_connection()
        self.is_connected = True

        return self.service

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.

        Returns
        -------
        response
            Status confirmation
        """
        response = StatusResponse(False)

        try:
            # Call the Google Analytics API
            service = self.connect()
            result = service.list_conversion_events(parent=f'properties/{self.property_id}')

            if result is not None:
                response.success = True
        except Exception as error:
            response.error_message = f'Error connecting to Google Analytics api: {error}.'
            log.logger.error(response.error_message)
            self.is_connected = False

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query_string: str = None) -> Response:
        ast = parse_sql(query_string)

        return self.query(ast)

    def get_api_url(self, endpoint):
        return f'{endpoint}/{self.property_id}'
