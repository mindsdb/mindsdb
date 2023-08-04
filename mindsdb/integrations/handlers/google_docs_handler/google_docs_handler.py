from mindsdb.integrations.handlers.google_docs_handler.google_docs_tables import GoogleDocGetDetailsTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.utilities.log import get_log
from mindsdb_sql import parse_sql

import requests
import os.path
import pandas as pd
import json
from collections import OrderedDict
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError



logger = get_log("integrations.google_docs_handler")

class GoogleDocs_Handler(APIHandler):
    """Google Docs handler implementation"""
    name = 'google_docs'

    def __init__(self, name=None, **kwargs):
        """Initialize the Google Docs handler.
        Parameters
        ----------
        name : str
            name of a handler instance
        """
        super().__init__(name)

        self.token = None
        self.connection = None
        self.connection_data = kwargs.get('connection_data', {})
        self.credentials_file = self.connection_data.get('credentials', None)
        self.scopes = ['https://www.googleapis.com/auth/documents.readonly']
        self.credentials = None
        self.is_connected = False
        self.parser = parse_sql
        
        google_doc_data = GoogleDocGetDetailsTable(self)
        self._register_table("get_doc_details", google_doc_data)


    def connect(self) -> StatusResponse:
        """Set up the connection required by the handler.
        Returns
        -------
        StatusResponse
            connection object
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
            with open('token.json', 'w') as token:
                token.write(self.credentials.to_json())
            self.connection = build('docs', 'v1', credentials=self.credentials)

        return self.connection


    def check_connection(self) -> StatusResponse:
        """Check connection to the handler.
        Returns
        -------
        StatusResponse
            Status confirmation
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            response.success = True
        except Exception as e:
            logger.error(f"Error connecting to Google Doc API: {e}!")
            response.error_message = e

        self.is_connected = response.success

        return response

    def native_query(self, query: str) -> StatusResponse:
        """Receive and process a raw query.
        Parameters
        ----------
        query : str
            query in a native format
        Returns
        -------
        StatusResponse
            Request status
        """
        ast = parse_sql(query, dialect="mindsdb")
        return self.query(ast)

