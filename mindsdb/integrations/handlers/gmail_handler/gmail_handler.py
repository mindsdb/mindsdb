import os
import base64
import re
from bs4 import BeautifulSoup
import pandas as pd
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pandas import DataFrame

from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from mindsdb.integrations.handlers.gmail_handler.gmail_table import GmailApiTable
from mindsdb.integrations.libs.api_handler import APIHandler, FuncParser
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)


class GmailHandler(APIHandler):

    def __init__(self, name: str, **kwargs):
        super().__init__(name=name)
        self.token = None
        self.service = None
        self.credentials_file = None
        self.scopes = ['https://www.googleapis.com/auth/gmail.readonly']
        # 'https://www.googleapis.com/auth/gmail.send',
        # 'https://www.googleapis.com/auth/gmail.compose']
        args = kwargs.get('connection_data', {})
        self.credentials = None
        if 'path_to_credentials_file' in args:
            self.credentials_file = args['path_to_credentials_file']
        self.is_connected = False
        gmails = GmailApiTable(self)
        self.emails = gmails
        self._register_table('emails', self.emails)

    def connect(self):
        if self.is_connected is True:
            return self.service
        try:
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
                self.service = build('gmail', 'v1', credentials=self.credentials)
                self.is_connected = True
                return self.service
            else:
                raise Exception("Credentials file not found")
        except Exception as e:
            print(e)
            raise Exception("Failed to connect to Gmail API")

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

    def native_query(self, query: str = None) -> Response:
        method_name, params = FuncParser().from_string(query)

        df = self.call_application_api(method_name, params)
        return Response(
            RESPONSE_TYPE.TABLE,
            data_frame=df
        )

    def call_application_api(self, method_name: str, query : str) -> DataFrame:
        if method_name == 'get_emails':
            return self.get_emails(query=query)
        else:
            raise Exception("Method not supported")

    def get_emails(self, query: str = None) -> DataFrame:
        service = self.connect()
        emails = pd.DataFrame(columns=self.emails.get_columns())
        try:
            results = service.users().messages().list(userId='me', q=query, maxResults=40).execute()
            messages = results.get('messages', [])
            for message in messages:
                msg = service.users().messages().get(userId='me', id=message['id']).execute()
                id = msg['id']
                thread_id = msg['threadId']
                label_ids = msg['labelIds']
                snippet = msg['snippet']
                history_id = msg['historyId']
                mimetype = msg['payload']['mimeType']
                filename = ''
                payload = msg['payload']
                sender = ''
                to = ''
                subject = ''
                date = ''
                for header in payload['headers']:
                    if header['name'] == 'Subject':
                        subject = header['value']
                    if header['name'] == 'From':
                        sender = header['value']
                    if header['name'] == 'To':
                        to = header['value']
                    if header['name'] == 'Date':
                        date = header['value']
                parts = payload.get('parts', None)
                body = ''
                if parts:
                    for part in parts:
                        if part['mimeType'] == 'text/plain':
                            body = part['body']['data']
                            body = base64.urlsafe_b64decode(body).decode('utf-8')
                        elif part['mimeType'] == 'text/html':
                            body = self.extract_html_body(part['body']['data'])
                        elif part['mimeType'] == 'multipart/alternative':
                            for subpart in part['parts']:
                                if subpart['mimeType'] == 'text/plain':
                                    body = subpart['body']['data']
                                    body = base64.urlsafe_b64decode(body).decode('utf-8')
                                if subpart['mimeType'] == 'text/html':
                                    body = self.extract_html_body(subpart['body']['data'])
                        else:
                            continue
                        body = re.sub(r'(?<!>)\s+(?!<)', ' ', body).strip()
                size_estimate = msg['sizeEstimate']
                emails = pd.concat([emails, pd.DataFrame([{'id': id, 'threadId': thread_id,
                                                           'labelIds': label_ids, 'snippet': snippet,
                                                           'historyId': history_id,
                                                           'mimeType': mimetype,
                                                           'filename': filename, 'Subject': subject,
                                                           'Sender': sender, 'To': to, 'Date': date,
                                                           'body': body, 'sizeEstimate': size_estimate}])],
                                   ignore_index=True)
            return emails
        except Exception as e:
            print(e)

    def extract_html_body(self, encoded_body):
        html_message = base64.urlsafe_b64decode(encoded_body).decode('utf-8')
        soup = BeautifulSoup(html_message, 'html.parser')
        # Extract the text from the HTML
        for element in soup(['style', 'script']):
            element.extract()

        # Extract the visible text from the HTML and remove whitespace characters
        text = soup.get_text().strip()
        return text
