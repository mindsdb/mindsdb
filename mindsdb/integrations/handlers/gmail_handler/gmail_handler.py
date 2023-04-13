import base64
import os
import json
from bs4 import BeautifulSoup
from typing import Any
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

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

    def __init__(self, name: str):
        super().__init__(name)
        self.credentials = None
        self.token = None
        self.service = None
        self.credentials_file = None
        self.scopes = None

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
            self.service = build('gmail', 'v1', credentials=self.credentials)

    def check_connection(self) -> StatusResponse:
        pass

    def native_query(self, query: Any) -> Response:
        pass

    def call_gmail_api(self, method: str = None):
        self.connect()
        api_method = getattr(self.service.users(), method)
        return api_method().list(userId='me', maxResults=15)

    def htmlToText(self,decoded_data):
        html_content = decoded_data.decode('UTF-8')
        # Parse the HTML content using Beautiful Soup
        soup = BeautifulSoup(html_content, 'html.parser')

        # Extract all the text from the HTML content
        text = soup.get_text()

        # Print the extracted text
        print(text)


gmail = GmailHandler(name="gmail")
gmail.credentials_file = "credentials.json"
gmail.scopes = ['https://www.googleapis.com/auth/gmail.readonly']
messages = gmail.call_gmail_api("messages").execute()
messages = messages['messages']
for message in messages:
    msg = gmail.service.users().messages().get(userId="me", id=message['id']).execute()
    encoded_data = msg['payload']['body'].get('data')
    if encoded_data!=None:
        decoded_data = base64.urlsafe_b64decode(encoded_data.encode('ASCII'))
        gmail.htmlToText(decoded_data)



    # parts = msg['payload'].get('parts')
    # # encoded_data = msg['body']['data']
    # # decoded_data = base64.urlsafe_b64decode(encoded_data.encode('ASCII'))
    # # print(decoded_data.decode('UTF-8'))
    # print(json.dumps(parts, indent=4))  # takes the email and prints the email in a json


