import json

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response
)

from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.integrations.libs.api_handler import APIHandler, APITable
from mindsdb_sql_parser import ast
from mindsdb.utilities import log
from mindsdb_sql_parser import parse_sql
from mindsdb.utilities.config import Config

import time
from typing import List
import pandas as pd

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from email.message import EmailMessage

from base64 import urlsafe_b64encode, urlsafe_b64decode

from mindsdb.integrations.utilities.handlers.auth_utilities.google import GoogleUserOAuth2Manager
from mindsdb.integrations.utilities.handlers.auth_utilities.exceptions import AuthException

DEFAULT_SCOPES = [
    'https://www.googleapis.com/auth/gmail.compose',
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.modify'
]

logger = log.getLogger(__name__)


class EmailsTable(APITable):
    """Implementation for the emails table for Gmail"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls emails from Gmail "users.messages.list" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Email matching the query

        Raises
        ------
        NotImplementedError
            If the query contains an unsupported operation or condition
        """

        conditions = extract_comparison_conditions(query.where)

        params = {}
        for op, arg1, arg2 in conditions:

            if op == 'or':
                raise NotImplementedError('OR is not supported')

            if arg1 in ['query', 'label_ids', 'include_spam_trash']:
                if op == '=':
                    if arg1 == 'query':
                        params['q'] = arg2
                    elif arg1 == 'label_ids':
                        params['labelIds'] = arg2.split(',')
                    else:
                        params['includeSpamTrash'] = arg2
                else:
                    raise NotImplementedError(f'Unknown op: {op}')

            else:
                raise NotImplementedError(f'Unknown clause: {arg1}')

        if query.limit is not None:
            params['maxResults'] = query.limit.value

        result = self.handler.call_gmail_api(
            method_name='list_messages',
            params=params
        )
        # filter targets
        columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                columns.append(target.parts[-1])
            else:
                raise NotImplementedError(f"Unknown query target {type(target)}")

        # columns to lower case
        columns = [name.lower() for name in columns]

        if len(result) == 0:
            return pd.DataFrame([], columns=columns)

        # add absent columns
        for col in set(columns) & set(result.columns) ^ set(columns):
            result[col] = None

        # filter by columns
        result = result[columns]

        # Rename columns
        for target in query.targets:
            if target.alias:
                result.rename(columns={target.parts[-1]: str(target.alias)}, inplace=True)

        return result

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """
        return [
            'id',
            'message_id',
            'thread_id',
            'label_ids',
            'sender',
            'to',
            'date',
            'subject',
            'snippet',
            'history_id',
            'size_estimate',
            'body',
            'attachments',
        ]

    def insert(self, query: ast.Insert):
        """Sends reply emails using the Gmail "users.messages.send" API

        Parameters
        ----------
        query : ast.Insert
           Given SQL INSERT query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        columns = [col.name for col in query.columns]

        supported_columns = {"message_id", "thread_id", "to_email", "subject", "body"}
        if not set(columns).issubset(supported_columns):
            unsupported_columns = set(columns).difference(supported_columns)
            raise ValueError(
                "Unsupported columns for create email: "
                + ", ".join(unsupported_columns)
            )

        for row in query.values:
            params = dict(zip(columns, row))

            if 'to_email' not in params:
                raise ValueError('"to_email" parameter is required to send an email')

            message = EmailMessage()
            message['To'] = params['to_email']
            message['Subject'] = params['subject'] if 'subject' in params else ''

            content = params['body'] if 'body' in params else ''
            message.set_content(content)

            # If threadId is present then add References and In-Reply-To headers
            # so that proper threading can happen
            if 'thread_id' in params and 'message_id' in params:
                message['In-Reply-To'] = params['message_id']
                message['References'] = params['message_id']

            encoded_message = urlsafe_b64encode(message.as_bytes()).decode()

            message = {
                'raw': encoded_message
            }

            if 'thread_id' in params:
                message['threadId'] = params['thread_id']
            self.handler.call_gmail_api('send_message', {'body': message})

    def delete(self, query: ast.Delete):
        """
        Deletes an event or events in the calendar.

        Args:
            query (ast.Delete): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        # Parse the query to get the conditions.
        conditions = extract_comparison_conditions(query.where)
        for op, arg1, arg2 in conditions:
            if op == 'or':
                raise NotImplementedError('OR is not supported')
            if arg1 == 'message_id':
                if op == '=':
                    self.handler.call_gmail_api('delete_message', {'id': arg2})
                else:
                    raise NotImplementedError(f'Unknown op: {op}')
            else:
                raise NotImplementedError(f'Unknown clause: {arg1}')

    def update(self, query: ast.Update) -> None:
        """Updates a label of a message.

        Args:
            query (ASTNode): The SQL query to parse.

        Raises:
            NotImplementedError: If the query contains an unsupported condition.
        """
        params = {}
        conditions = extract_comparison_conditions(query.where)
        for op, arg1, arg2 in conditions:
            if op == 'or':
                raise NotImplementedError('OR is not supported')
            if arg1 == 'id':
                if op == '=':
                    params['id'] = arg2
                else:
                    raise NotImplementedError(f'Unknown op: {op}')
            else:
                raise NotImplementedError(f'Unknown clause: {arg1}')
        request_body = {}
        values = query.update_columns.items()
        data_list = list(values)
        add_label = []
        remove_label = []
        for key, value in data_list:
            if key == 'addLabel':
                add_label.append(str(value)[1:-1])
            elif key == 'removeLabel':
                remove_label.append(str(value)[1:-1])
            else:
                raise NotImplementedError(f'Unknown clause: {key}')
        if add_label:
            request_body['addLabelIds'] = add_label
        if remove_label:
            request_body['removeLabelIds'] = remove_label
        params['body'] = request_body
        self.handler.call_gmail_api('update_message', params)


class GmailHandler(APIHandler):
    """A class for handling connections and interactions with the Gmail API.

    Attributes:
        credentials_file (str): The path to the Google Auth Credentials file for authentication
        and interacting with the Gmail API on behalf of the uesr.

        scopes (List[str], Optional): The scopes to use when authenticating with the Gmail API.
    """

    def __init__(self, name=None, **kwargs):
        super().__init__(name)
        self.connection_args = kwargs.get('connection_data', {})

        self.token_file = None
        self.max_page_size = 500
        self.max_batch_size = 100
        self.service = None
        self.is_connected = False

        self.handler_storage = kwargs['handler_storage']

        self.credentials_url = self.connection_args.get('credentials_url', None)
        self.credentials_file = self.connection_args.get('credentials_file', None)
        if self.connection_args.get('credentials'):
            self.credentials_file = self.connection_args.pop('credentials')
        if not self.credentials_file and not self.credentials_url:
            # try to get from config
            gm_config = Config().get('handlers', {}).get('gmail', {})
            secret_file = gm_config.get('credentials_file')
            secret_url = gm_config.get('credentials_url')
            if secret_file:
                self.credentials_file = secret_file
            elif secret_url:
                self.credentials_url = secret_url

        self.scopes = self.connection_args.get('scopes', DEFAULT_SCOPES)

        emails = EmailsTable(self)
        self.emails = emails
        self._register_table('emails', emails)

    def connect(self):
        """Authenticate with the Gmail API using the credentials file.

        Returns
        -------
        service: object
            The authenticated Gmail API service object.
        """
        if self.is_connected and self.service is not None:
            return self.service

        google_oauth2_manager = GoogleUserOAuth2Manager(self.handler_storage, self.scopes, self.credentials_file, self.credentials_url, self.connection_args.get('code'))
        creds = google_oauth2_manager.get_oauth2_credentials()

        self.service = build('gmail', 'v1', credentials=creds)

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
            # Call the Gmail API
            service = self.connect()

            result = service.users().getProfile(userId='me').execute()

            if result and result.get('emailAddress', None) is not None:
                response.success = True
                response.copy_storage = True
        except AuthException as error:
            response.error_message = str(error)
            response.redirect_url = error.auth_url
            return response

        except HttpError as error:
            response.error_message = f'Error connecting to Gmail api: {error}.'
            logger.error(response.error_message)

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query_string: str = None) -> Response:
        ast = parse_sql(query_string)

        return self.query(ast)

    def _parse_parts(self, parts, attachments):
        if not parts:
            return ''

        body = ''
        for part in parts:
            if part['mimeType'] == 'text/plain':
                part_body = part.get('body', {}).get('data', '')
                body += urlsafe_b64decode(part_body).decode('utf-8')
            elif part['mimeType'] == 'multipart/alternative' or 'parts' in part:
                # Recursively iterate over nested parts to find the plain text body
                body += self._parse_parts(part['parts'], attachments)
            elif part.get('filename') and part.get('body') and part.get('body').get('attachmentId'):
                # For now just store the attachment details
                attachments.append({
                    'filename': part['filename'],
                    'mimeType': part['mimeType'],
                    'attachmentId': part['body']['attachmentId']
                })
            else:
                logger.debug(f"Unhandled mimeType: {part['mimeType']}")

        return body

    def _parse_message(self, data, message, exception):
        if exception:
            logger.error(f'Exception in getting full email: {exception}')
            return

        payload = message['payload']
        headers = payload.get("headers", [])
        parts = payload.get("parts")

        row = {
            'id': message['id'],
            'thread_id': message['threadId'],
            'label_ids': message.get('labelIds', []),
            'snippet': message.get('snippet', ''),
            'history_id': message['historyId'],
            'size_estimate': message.get('sizeEstimate', 0),
        }

        for header in headers:
            key = header['name'].lower()
            value = header['value']

            if key in ['to', 'subject', 'date']:
                row[key] = value
            elif key == 'from':
                row['sender'] = value
            elif key == 'message-id':
                row['message_id'] = value

        attachments = []
        row['body'] = self._parse_parts(parts, attachments)
        row['attachments'] = json.dumps(attachments)
        data.append(row)

    def _get_messages(self, data, messages):
        batch_req = self.service.new_batch_http_request(
            lambda id, response, exception: self._parse_message(data, response, exception))
        for message in messages:
            batch_req.add(self.service.users().messages().get(userId='me', id=message['id']))

        batch_req.execute()

    def get_attachments(self, result):
        for index, email in result.iterrows():
            attachments = json.loads(email['attachments'])
            for attachment in attachments:
                attachment_id = attachment['attachmentId']
                filename = attachment['filename']
                attachment_data = self.service.users().messages().attachments().get(
                    userId='me', messageId=email['id'], id=attachment_id).execute()
                file_data = attachment_data['data']
                file_data = file_data.replace('-', '+').replace('_', '/')
                file_data = urlsafe_b64decode(file_data)
                with open(filename, 'wb') as f:
                    f.write(file_data)

    def _handle_list_messages_response(self, data, messages):
        total_pages = len(messages) // self.max_batch_size
        for page in range(total_pages):
            self._get_messages(data, messages[page * self.max_batch_size:(page + 1) * self.max_batch_size])

        # Get the remaining messsages, if any
        if len(messages) % self.max_batch_size > 0:
            self._get_messages(data, messages[total_pages * self.max_batch_size:])

    def call_gmail_api(self, method_name: str = None, params: dict = None) -> pd.DataFrame:
        """Call Gmail API and map the data to pandas DataFrame
        Args:
            method_name (str): method name
            params (dict): query parameters
        Returns:
            DataFrame
        """
        service = self.connect()
        if method_name == 'list_messages':
            method = service.users().messages().list
        elif method_name == 'send_message':
            method = service.users().messages().send
        elif method_name == "delete_message":
            method = service.users().messages().trash
        elif method_name == 'update_message':
            method = service.users().messages().modify
        else:
            raise NotImplementedError(f'Unknown method_name: {method_name}')

        left = None
        count_results = None
        if 'maxResults' in params:
            count_results = params['maxResults']

        params['userId'] = 'me'

        data = []
        limit_exec_time = time.time() + 60

        while True:
            if time.time() > limit_exec_time:
                raise RuntimeError('Handler request timeout error')

            if count_results is not None:
                left = count_results - len(data)
                if left == 0:
                    break
                elif left < 0:
                    # got more results that we need
                    data = data[:left]
                    break

                if left > self.max_page_size:
                    params['maxResults'] = self.max_page_size
                else:
                    params['maxResults'] = left

            logger.debug(f'Calling Gmail API: {method_name} with params ({params})')

            resp = method(**params).execute()

            if 'messages' in resp:
                self._handle_list_messages_response(data, resp['messages'])
            elif isinstance(resp, dict):
                data.append(resp)

            if count_results is not None and 'nextPageToken' in resp:
                params['pageToken'] = resp['nextPageToken']
            else:
                break

        df = pd.DataFrame(data)

        return df
