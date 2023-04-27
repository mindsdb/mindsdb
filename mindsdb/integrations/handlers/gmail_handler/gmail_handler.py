import base64
import os
import pandas as pd

from bs4 import BeautifulSoup
from typing import Any
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response
)


class GmailHandler(APIHandler):
    """A Class for handling connections and interactions with the Gmail API

    Attributes:
    Scopes:Authorization scopes express the permissions you request users to authorize for your app
    service(googleapiclient.discovery):A client object interacting with Google's discovery based APIs
    credentials_file:The filename of the credentials file that was generated from Google
    credentials(google.oauth2.credentials):Google oauth2 object that keeps the credentials of you api connection
    is_connected(bool): Whether the API client is connected to Gmail.

    """

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.token = None
        self.service = None
        self.credentials_file = None
        self.scopes = None
        args = kwargs.get('connection_data', {})
        self.credentials = {}
        if 'path_to_credentials_file' in args:
            self.credentials_file = args['path_to_credentials_file']
        if 'scopes' in args:
            self.scopes = args['scopes']

    def connect(self):
        """Authenticate with Gmail API using OAuth 2.0 Client ID that you create on Google Apis page.The implementation uses trhe credentials.json file that google give you in order to verify you."""
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
                        'credentials.json', self.scopes)
                    self.credentials = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(self.credentials.to_json())
            self.is_connected = True
            self.service = build('gmail', 'v1', credentials=self.credentials)

    def check_connection(self) -> StatusResponse:
        """Checks connection to Gmail Api by getting the user's profile.

        Returns StatusResponse indicating whether the handler is connected."""
        response = StatusResponse(False)
        try:
            # Call the Gmail API to get the user's profile
            profile = self.service.users().getProfile(userId='me').execute()
            # Check if the API call was successful
            if 'emailAddress' in profile:
                response.success = True
        except HttpError as error:
            response.error_message = error
        return response

    def native_query(self, query: Any) -> Response:
        pass

    def get_last_n_emails(self, numberOfEmails):
        method = "list"
        params = {'userId': 'me', 'maxResults': numberOfEmails}
        result = self.call_gmail_api(method, **params)
        messages = result.execute()
        messages = messages['messages']
        data = []
        for message in messages:
            msg = self.service.users().messages().get(userId="me", id=message['id']).execute()
            payload = msg['payload']
            headers = payload['headers']
            subject = ''
            sender = ''
            date = ''
            for header in headers:
                if header['name'] == 'Subject':
                    subject = header['value']
                elif header['name'] == 'From':
                    sender = header['value']
                elif header['name'] == 'Date':
                    date = header['value']
            # get the email body
            parts = payload.get('parts')
            text = ''
            if parts:
                for part in parts:
                    if part['mimeType'].startswith('text'):
                        body = part['body'].get('data')
                        if body:
                            text = self.extract_text_from_email_body(body, part['mimeType'])
            data.append({'Subject': subject, 'From': sender, 'Date': date, 'Body': text})
        return pd.DataFrame(data)

    def call_gmail_api(self, method: str = None, **kwargs):
        self.connect()
        # TODO check connection
        parts = method.split('.')
        api_method = self.service.users().messages()
        for part in parts:
            api_method = getattr(api_method, part)
        return api_method(**kwargs)

    def extract_text_from_email_body(self, email_body, mime_type):
        if mime_type == 'text/plain':
            # if MIME type is plain text, simply print the decoded body
            decoded_data = base64.urlsafe_b64decode(email_body.encode('ASCII'))
            return decoded_data.decode('UTF-8')
        elif mime_type == 'text/html':
            # if MIME type is HTML, extract text content using beautifulsoup4
            decoded_data = base64.urlsafe_b64decode(email_body.encode('ASCII'))
            soup = BeautifulSoup(decoded_data.decode('UTF-8'), 'html.parser')
            text = soup.get_text()
            text = "\n".join([line.strip() for line in text.split("\n") if line.strip()])
            return text
        else:
            # handle other MIME types here as necessary
            return None

