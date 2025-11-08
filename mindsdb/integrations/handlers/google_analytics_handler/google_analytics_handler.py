from mindsdb_sql_parser import parse_sql
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.utilities import log
from mindsdb.integrations.handlers.google_analytics_handler.google_analytics_tables import ConversionEventsTable
from mindsdb.integrations.handlers.google_analytics_handler.google_analytics_data_tables import (
    ReportsTable,
    RealtimeReportsTable,
    MetadataTable,
)
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)

import json
import os

from google.analytics.admin_v1beta import AnalyticsAdminServiceClient
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError

DEFAULT_SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
                  'https://www.googleapis.com/auth/analytics.edit',
                  'https://www.googleapis.com/auth/analytics'
                  ]

logger = log.getLogger(__name__)


class GoogleAnalyticsHandler(APIHandler):
    """A class for handling connections and interactions with the Google Analytics Admin API and Data API.

    This handler supports both the Admin API (for managing conversion events) and the Data API (for
    running reports and accessing analytics data).

    Attributes:
        property_id (str): The Google Analytics 4 property ID.
        credentials_file (str): The path to the Google Auth Credentials file for authentication
            and interacting with the Google Analytics API on behalf of the user.
        credentials_json (dict): Alternative to credentials_file, provide credentials as a dictionary.
        scopes (List[str], Optional): The scopes to use when authenticating with the Google Analytics API.

    Tables:
        Admin API:
            - conversion_events: Manage conversion events (SELECT, INSERT, UPDATE, DELETE)

        Data API:
            - reports: Run standard GA4 reports with dimensions and metrics (SELECT)
            - realtime_reports: Run realtime reports for current user activity (SELECT)
            - metadata: Fetch available dimensions and metrics (SELECT)
    """

    name = 'google_analytics'

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.page_size = 500
        self.connection_args = kwargs.get('connection_data', {})
        self.handler_storage = kwargs.get('handler_storage')
        self.property_id = self.connection_args['property_id']
        if self.connection_args.get('credentials'):
            self.credentials_file = self.connection_args.pop('credentials')

        self.scopes = self.connection_args.get('scopes', DEFAULT_SCOPES)
        self.service = None  # Admin API client (for backward compatibility)
        self.admin_service = None  # Admin API client
        self.data_service = None  # Data API client
        self.is_connected = False

        # Register Admin API tables
        conversion_events = ConversionEventsTable(self)
        self.conversion_events = conversion_events
        self._register_table('conversion_events', conversion_events)

        # Register Data API tables
        reports = ReportsTable(self)
        self.reports = reports
        self._register_table('reports', reports)

        realtime_reports = RealtimeReportsTable(self)
        self.realtime_reports = realtime_reports
        self._register_table('realtime_reports', realtime_reports)

        metadata = MetadataTable(self)
        self.metadata = metadata
        self._register_table('metadata', metadata)

    def _store_credentials(self, credentials_data: dict) -> None:
        """
        Store credentials securely in encrypted storage

        Args:
            credentials_data: Service account credentials as dictionary
        """
        if not hasattr(self, 'handler_storage') or not self.handler_storage:
            return

        try:
            self.handler_storage.encrypted_json_set("ga_credentials", credentials_data)
        except Exception as e:
            logger.warning(f"Failed to store credentials: {e}")

    def _load_stored_credentials(self) -> dict:
        """
        Load stored credentials from encrypted storage

        Returns:
            dict: Stored credentials or None if not found
        """
        if not hasattr(self, 'handler_storage') or not self.handler_storage:
            return None

        try:
            return self.handler_storage.encrypted_json_get("ga_credentials")
        except Exception as e:
            logger.debug(f"No stored credentials found: {e}")
            return None

    def _get_creds_json(self):
        # First, try to load from encrypted storage
        stored_creds = self._load_stored_credentials()
        if stored_creds:
            return stored_creds

        # If not in storage, load from connection args and store securely
        if 'credentials_file' in self.connection_args:
            if os.path.isfile(self.connection_args['credentials_file']) is False:
                raise Exception("credentials_file must be a file path")
            with open(self.connection_args['credentials_file']) as source:
                info = json.load(source)
            # Store credentials for future use
            self._store_credentials(info)
            return info
        elif 'credentials_json' in self.connection_args:
            info = json.loads(self.connection_args['credentials_json'])
            if not isinstance(info, dict):
                raise Exception("credentials_json has to be dict")
            info['private_key'] = info['private_key'].replace('\\n', '\n')
            # Store credentials for future use
            self._store_credentials(info)
            return info
        else:
            raise Exception('Connection args have to content ether credentials_file or credentials_json')

    def create_connection(self):
        info = self._get_creds_json()
        creds = service_account.Credentials.from_service_account_info(info=info, scopes=self.scopes)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())

        # Create both Admin API and Data API clients with same credentials
        admin_client = AnalyticsAdminServiceClient(credentials=creds)
        data_client = BetaAnalyticsDataClient(credentials=creds)

        return admin_client, data_client

    def connect(self):
        """
        Authenticate with the Google Analytics Admin API and Data API using the credential file.

        Returns
        -------
        service: object
            The authenticated Google Analytics Admin API service object (for backward compatibility).
        """
        if self.is_connected is True:
            return self.service

        self.admin_service, self.data_service = self.create_connection()
        self.service = self.admin_service  # For backward compatibility
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
        except HttpError as error:
            response.error_message = f'Error connecting to Google Analytics api: {error}.'
            log.logger.error(response.error_message)

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query_string: str = None) -> Response:
        ast = parse_sql(query_string)

        return self.query(ast)

    def get_api_url(self, endpoint):
        return f'{endpoint}/{self.property_id}'
