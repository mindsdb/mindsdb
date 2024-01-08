from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response
)
from mindsdb.integrations.libs.api_handler import APIHandler, FuncParser
from mindsdb.utilities import log
from mindsdb.integrations.handlers.google_analytics_handler.google_analytics_tables import ConversionEventsTable

import os
import pandas as pd

from google.analytics.admin_v1beta import AnalyticsAdminServiceClient, ListConversionEventsRequest, ConversionEvent, \
    CreateConversionEventRequest
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
        """Check connection to the handler.

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

    def native_query(self, query: str = None) -> Response:
        """
        Receive raw query and act upon it somehow.
        Args:
            query (Any): query in native format (str for sql databases,
                dict for mongo, api's json etc.)
        Returns:
            HandlerResponse
        """
        method_name, params = FuncParser().from_string(query)

        df = self.call_application_api(method_name, params)

        return Response(
            RESPONSE_TYPE.TABLE,
            data_frame=df
        )

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
        conversion_events = pd.DataFrame(columns=self.conversion_events.get_columns())

        while True:
            request = ListConversionEventsRequest(parent=f'properties/{self.property_id}',
                                                  page_token=page_token, **params)
            result = service.list_conversion_events(request)
            conversion_events_data = self.extract_conversion_events_data(result.conversion_events)
            conversion_events = self.concat_dataframes(conversion_events, conversion_events_data)

            page_token = result.next_page_token
            if not page_token:
                break
        return conversion_events

    def create_conversion_event(self, params: dict = None):
        """
        Create a conversion event in your property.
        Args:
            params (dict): query parameters
        Returns:
            DataFrame
        """
        service = self.connect()
        conversion_events = pd.DataFrame(columns=self.conversion_events.get_columns())

        conversion_event = ConversionEvent(
            event_name=params['event_name'],
            counting_method=params['countingMethod']
        )
        request = CreateConversionEventRequest(conversion_event=conversion_event,
                                               parent=f'properties/{self.property_id}')
        result = service.create_conversion_event(request)
        conversion_events_data = self.extract_conversion_events_data([result])
        conversion_events = self.concat_dataframes(conversion_events, conversion_events_data)

        return conversion_events

    @staticmethod
    def extract_conversion_events_data(conversion_events):
        """
        Extract conversion events data and return a list of lists.
        Args:
            conversion_events: List of ConversionEvent objects
        Returns:
            List of lists containing conversion event data
        """
        conversion_events_data = []
        for conversion_event in conversion_events:
            data_row = [
                conversion_event.name,
                conversion_event.event_name,
                conversion_event.create_time,
                conversion_event.deletable,
                conversion_event.custom,
                conversion_event.ConversionCountingMethod(conversion_event.counting_method).name,
            ]
            conversion_events_data.append(data_row)
        return conversion_events_data

    def concat_dataframes(self, existing_df, data):
        """
        Concatenate existing DataFrame with new data.
        Args:
            existing_df: Existing DataFrame
            data: New data to be added to the DataFrame
        Returns:
            Concatenated DataFrame
        """
        return pd.concat(
            [existing_df, pd.DataFrame(data, columns=self.conversion_events.get_columns())],
            ignore_index=True
        )

    def call_application_api(self, method_name: str = None, params: dict = None) -> pd.DataFrame:
        """
        Call Google Analytics Admin API and map the data to pandas DataFrame
        Args:
            method_name (str): method name
            params (dict): query parameters
        Returns:
            DataFrame
        """
        if method_name == 'get_conversion_events':
            return self.get_conversion_events(params)
        elif method_name == 'create_conversion_event':
            return self.create_conversion_event(params)
        else:
            raise NotImplementedError(f'Unknown method {method_name}')
