from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.utilities import log
from mindsdb.integrations.handlers.google_analytics_handler.google_analytics_tables import ConversionEventsTable

import os
import pandas as pd

from google.analytics.admin_v1beta import AnalyticsAdminServiceClient, ListConversionEventsRequest, ConversionEvent, \
    CreateConversionEventRequest, UpdateConversionEventRequest, DeleteConversionEventRequest
from google.oauth2 import service_account
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

        scopes (List[str], Optional): The scopes to use when authenticating with the Google Analytics API.
    """

    name = 'google_analytics'

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.connection_args = kwargs.get('connection_data', {})

        self.credentials_file = self.connection_args['credentials_file']
        self.property_id = self.connection_args['property_id']
        if self.connection_args.get('credentials'):
            self.credentials_file = self.connection_args.pop('credentials')

        self.scopes = self.connection_args.get('scopes', DEFAULT_SCOPES)
        self.service = None
        self.is_connected = False
        conversion_events = ConversionEventsTable(self)
        self.conversion_events = conversion_events
        self._register_table('conversion_events', conversion_events)

    def create_connection(self):
        creds = None

        if os.path.isfile(self.credentials_file):
            creds = service_account.Credentials.from_service_account_file(self.credentials_file, scopes=self.scopes)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            elif not os.path.isfile(self.credentials_file):
                raise Exception('Credentials must be a file path')

        return AnalyticsAdminServiceClient(credentials=creds)

    def connect(self):
        """Authenticate with the Google Analytics Admin API using the credentials file.

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
        StatusResponse
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

    def get_conversion_events(self, params: dict = None) -> pd.DataFrame:
        """
        Get conversion events from Google Analytics Admin API
        Args:
            params (dict): query parameters
        Returns:
            DataFrame
        """
        service = self.connect()
        page_token = None

        while True:
            request = ListConversionEventsRequest(parent=f'properties/{self.property_id}',
                                                  page_token=page_token, **params)
            result = service.list_conversion_events(request)

            page_token = result.next_page_token
            if not page_token:
                break
        return result

    def create_conversion_event(self, params: dict = None):
        """
        Create a conversion event in your property.
        Args:
            params (dict): query parameters
        Returns:
            DataFrame
        """
        service = self.connect()

        conversion_event = ConversionEvent(
            event_name=params['event_name'],
            counting_method=params['countingMethod']
        )
        request = CreateConversionEventRequest(conversion_event=conversion_event,
                                               parent=f'properties/{self.property_id}')
        result = service.create_conversion_event(request)

        return result

    def update_conversion_event(self, params: dict = None):
        """
        Update a conversion event in your property.
        Args:
            params (dict): query parameters
        Returns:
            DataFrame
        """
        service = self.connect()

        conversion_event = ConversionEvent(
            name=params['name'],
            counting_method=params['countingMethod']
        )
        request = UpdateConversionEventRequest(conversion_event=conversion_event, update_mask='*')
        result = service.update_conversion_event(request)

        return result

    def delete_conversion_event(self, params: dict = None):
        """
        Delete a conversion event in your property.
        Args:
            params (dict): query parameters
        """
        service = self.connect()
        request = DeleteConversionEventRequest(name=params['name'])
        service.delete_conversion_event(request)
