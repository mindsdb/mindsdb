import os.path
import json
import pandas as pd
import pytz
from datetime import datetime, timedelta

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import Resource
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from mindsdb.integrations.handlers.google_fit_handler.google_fit_tables import GoogleFitTable
from mindsdb.integrations.libs.api_handler import APIHandler,FuncParser
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)
epoch0 = datetime(1970, 1, 1, tzinfo=pytz.utc)
SCOPES = ['https://www.googleapis.com/auth/fitness.activity.read']

class GoogleFitHandler(APIHandler):

    def __init__(self, name: str):
        super().__init__(name)

        args = kwargs.get('connection_data', {})
        self.connection_args = {}
        handler_config = Config().get('google_handler', {})
        #TODO: make sure the arguments can read a list from user input when the database is created, since "redirect_uris" is a list.
        for k in ['client_id', 'project_id', 'auth_uri',
                  'token_uri', 'auth_provider_x509_cert_url', 'client_secret',"redirect_uris"]:
            if k in args:
                self.connection_args[k] = args[k]
            elif f'GOOGLE_FIT_{k.upper()}' in os.environ:
                self.connection_args[k] = os.environ[f'GOOGLE_FIT_{k.upper()}']
            elif k in handler_config:
                self.connection_args[k] = handler_config[k]
        
        self.api = None
        self.is_connected = False

        aggregated_data = GoogleFitTable(self)
        self._register_table('aggregated_data', aggregated_data)

    def connect(self) -> Resource:
        if self.is_connected is True and self.api:
            return self.api
        if len(self.connection_args) == 6:
            credentialDict = {"installed":self.connection_args}
            f = open("credentials.json", "a")
            f.write(json.dumps(connection_args).replace(" ", ""))
            f.close()
        
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
        self.api = fit_service = build('fitness', 'v1', credentials=creds)
        
        self.is_connected = True
        return self.api

    def check_connection(self) -> HandlerStatusResponse:
    """ Check connection to the handler
    Returns:
        HandlerStatusResponse
    """

    def retrieve_data(service, startTimeMillis, endTimeMillis, dataSourceId):
        return service.users().dataset().aggregate(userId="me", body={
            "aggregateBy": [{
                "dataTypeName": "com.google.step_count.delta",
                "dataSourceId": dataSourceId
            }],
            "bucketByTime": {"durationMillis": 86400000},
            "startTimeMillis": startTimeMillis,
            "endTimeMillis": endTimeMillis
        }).execute()

    def native_query(self, query: str = None) -> Response:
    """Receive raw query and act upon it somehow.
    Args:
        query (Any): query in native format (str for sql databases,
            dict for mongo, api's json etc)
    Returns:
        HandlerResponse
    """

    def call_google_fit_api(self, method_name:str = None, params:dict = None) -> DataFrame:
    """Receive query as AST (abstract syntax tree) and act upon it somehow.
    Args:
        query (ASTNode): sql query represented as AST. May be any kind
            of query: SELECT, INSERT, DELETE, etc
    Returns:
        DataFrame
    """