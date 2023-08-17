import os
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from .google_calendar_tables import GoogleCalendarEventsTable
from mindsdb.integrations.libs.api_handler import APIHandler, FuncParser
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)
from mindsdb.utilities import log

logger = log.getLogger(__name__)

class GoogleCalendarHandler(APIHandler):
    """
        A class for handling connections and interactions with the Google Calendar API.
    """
    name = 'google_calendar'

    def __init__(self, name: str, **kwargs):
        """ constructor
        Args:
            name (str): the handler name
            credentials_file (str): The path to the credentials file.
            scopes (list): The list of scopes to use for authentication.
            is_connected (bool): Whether the API client is connected to Google Calendar.
            events (GoogleCalendarEventsTable): The `GoogleCalendarEventsTable` object for interacting with the events table.
        """
        super().__init__(name)

        self.token = None
        self.service = None
        self.connection_data = kwargs.get('connection_data', {})
        self.credentials_file = self.connection_data.get('credentials', None)
        self.scopes = ['https://www.googleapis.com/auth/calendar', 
              'https://www.googleapis.com/auth/calendar.events',
              'https://www.googleapis.com/auth/calendar.readonly'
            ]
        self.credentials = None
        self.is_connected = False
        events = GoogleCalendarEventsTable(self)
        self.events = events
        self._register_table('events', events)

    def connect(self):
        """
        Set up any connections required by the handler
        Should return output of check_connection() method after attempting
        connection. Should switch self.is_connected.
        Returns:
            HandlerStatusResponse
        """
        if self.is_connected is True:
            return self.service
        if self.credentials_file:
            if os.path.exists('token.json'):
                self.credentials = Credentials.from_authorized_user_file('token.json', self.scopes)
            if not self.credentials or not self.credentials.valid:
                if self.credentials and self.credentials.expired and self.credentials.refresh_token:
                    self.credentials.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file(
                        self.credentials_file, self.scopes)
                    self.credentials = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(self.credentials.to_json())
            self.service = build('calendar', 'v3', credentials=self.credentials)
        return self.service

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler
        Returns:
            HandlerStatusResponse
        """

        response = StatusResponse(False)

        try:
            service = self.connect()
            response.success = True
        except Exception as e:
            logger.error(f'Error connecting to Google Calendar API: {e}!')
            response.error_message = e

        self.is_connected = response.success
        return response

    def native_query(self, query: str = None) -> Response:
        """
        Receive raw query and act upon it somehow.
        Args:
            query (Any): query in native format (str for sql databases,
                dict for mongo, api's json etc)
        Returns:
            HandlerResponse
        """
        method_name, params = FuncParser().from_string(query)

        df = self.call_application_api(method_name, params)

        return Response(
            RESPONSE_TYPE.TABLE,
            data_frame=df
        )

    def get_events(self, params: dict = None) -> pd.DataFrame:
        """
        Get events from Google Calendar API
        Args:
            params (dict): query parameters
        Returns:
            DataFrame
        """
        service = self.connect()
        page_token = None
        events = pd.DataFrame(columns=self.events.get_columns())
        while True:
            events_result = service.events().list(calendarId='primary', pageToken=page_token, **params).execute()
            events = pd.concat(
                [events, pd.DataFrame(events_result.get('items', []), columns=self.events.get_columns())],
                ignore_index=True
            )
            page_token = events_result.get('nextPageToken')
            if not page_token:
                break
        return events

    def create_event(self, params: dict = None) -> pd.DataFrame:
        """
        Create an event in the calendar.
        Args:
            params (dict): query parameters
        Returns:
            DataFrame
        """
        service = self.connect()
        # Check if 'attendees' is a string and split it into a list
        if isinstance(params['attendees'], str):
            params['attendees'] = params['attendees'].split(',')

        event = {
            'summary': params['summary'],
            'location': params['location'],
            'description': params['description'],
            'start': {
                'dateTime': params['start']['dateTime'],
                'timeZone': params['start']['timeZone'],
            },
            'end': {
                'dateTime': params['end']['dateTime'],
                'timeZone': params['end']['timeZone'],
            },
            'recurrence': [
                'RRULE:FREQ=DAILY;COUNT=1'
            ],
            'attendees': [{'email': attendee['email']} for attendee in (params['attendees'] 
                            if isinstance(params['attendees'], list) else [params['attendees']])],
            'reminders': {
                'useDefault': False,
                'overrides': [
                    {'method': 'email', 'minutes': 24 * 60},
                    {'method': 'popup', 'minutes': 10},
                ],
            },
        }

        event = service.events().insert(calendarId='primary', 
                                        body=event).execute()
        return pd.DataFrame([event], columns=self.events.get_columns())

    def update_event(self, params: dict = None) -> pd.DataFrame:
        """
        Update event or events in the calendar.
        Args:
            params (dict): query parameters
        Returns:
            DataFrame
        """
        service = self.connect()
        df = pd.DataFrame(columns=['eventId', 'status'])
        if params['event_id']:
            start_id = int(params['event_id'])
            end_id = start_id + 1
        elif not params['start_id']:
            start_id = int(params['end_id']) - 10
        elif not params['end_id']:
            end_id = int(params['start_id']) + 10
        else:
            start_id = int(params['start_id'])
            end_id = int(params['end_id'])

        for i in range(start_id, end_id):
            event = service.events().get(calendarId='primary', eventId=i).execute()
            if params['summary']:
                event['summary'] = params['summary']
            if params['location']:
                event['location'] = params['location']
            if params['description']:
                event['description'] = params['description']
            if params['start']:
                event['start']['dateTime'] = params['start']['dateTime']
                event['start']['timeZone'] = params['start']['timeZone']
            if params['end']:
                event['end']['dateTime'] = params['end']['dateTime']
                event['end']['timeZone'] = params['end']['timeZone']
            if params['attendees']:
                event['attendees'] = [{'email': attendee} for attendee in params['attendees'].split(',')]
            updated_event = service.events().update(calendarId='primary', eventId=event['id'], body=event).execute()
            df = pd.concat([df, pd.DataFrame([{'eventId': updated_event['id'], 'status': 'updated'}])],
                           ignore_index=True)

        return df

    def delete_event(self, params):
        """
        Delete event or events in the calendar.
        Args:
            params (dict): query parameters
        Returns:
            DataFrame
        """
        service = self.connect()
        if params['event_id']:
            service.events().delete(calendarId='primary', eventId=params['event_id']).execute()
            return pd.DataFrame([{'eventId': params['event_id'], 'status': 'deleted'}])
        else:
            df = pd.DataFrame(columns=['eventId', 'status'])
            if not params['start_id']:
                start_id = int(params['end_id']) - 10
            elif not params['end_id']:
                end_id = int(params['start_id']) + 10
            else:
                start_id = int(params['start_id'])
                end_id = int(params['end_id'])
            for i in range(start_id, end_id):
                service.events().delete(calendarId='primary', eventId=str(i)).execute()
                df = pd.concat([df, pd.DataFrame([{'eventId': str(i), 'status': 'deleted'}])], ignore_index=True)
            return df

    def call_application_api(self, method_name: str = None, params: dict = None) -> pd.DataFrame:
        """
        Call Google Calendar API and map the data to pandas DataFrame
        Args:
            method_name (str): method name
            params (dict): query parameters
        Returns:
            DataFrame
        """
        if method_name == 'get_events':
            return self.get_events(params)
        elif method_name == 'create_event':
            return self.create_event(params)
        elif method_name == 'update_event':
            return self.update_event(params)
        elif method_name == 'delete_event':
            return self.delete_event(params)
        else:
            raise NotImplementedError(f'Unknown method {method_name}')
