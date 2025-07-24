import os.path
import json
import pandas as pd
import pytz
from tzlocal import get_localzone
from datetime import datetime

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import Resource
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from mindsdb_sql_parser import parse_sql

from mindsdb.utilities import log
from mindsdb.integrations.handlers.google_fit_handler.google_fit_tables import GoogleFitTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)

SCOPES = ['https://www.googleapis.com/auth/fitness.activity.read']
DATE_FORMAT = '%Y-%m-%d'

logger = log.getLogger(__name__)


class GoogleFitHandler(APIHandler):

    def __init__(self, name: str = None, **kwargs):
        super().__init__(name)
        args = kwargs.get('connection_data', {})
        self.connection_args = {}
        self.credentials_path = None
        if 'service_account_file' in args:
            if os.path.isfile(args['service_account_file']) is False:
                raise Exception("service_account_file must be a path to the credentials.json file")
            self.credentials_path = args['service_account_file']
        elif 'service_account_json' in args:
            self.connection_args = args['service_account_json']
            if not isinstance(self.connection_args, dict) or (('redirect_uris' not in self.connection_args.keys()) and len(self.connection_args) != 6) or ('redirect_uris' in self.connection_args.keys()) and len(self.connection_args) != 7:
                raise Exception("service_account_json has to be a dictionary with all 6 required fields")
            self.connection_args['redirect_uris'] = ['http://localhost']
            self.credentials_path = 'mindsdb/integrations/handlers/google_fit_handler/credentials.json'
        else:
            raise Exception('Connection args have to content ether service_account_file or service_account_json')

        self.api = None
        self.is_connected = False

        aggregated_data = GoogleFitTable(self)
        self._register_table('aggregated_data', aggregated_data)

    def connect(self) -> Resource:
        if self.is_connected is True and self.api:
            return self.api
        if self.connection_args:
            credentialDict = {"installed": self.connection_args}
            f = open(self.credentials_path, "w")
            f.write(json.dumps(credentialDict).replace(" ", ""))
            f.close()

        creds = None

        if os.path.isfile('mindsdb/integrations/handlers/google_fit_handler/token.json'):
            creds = Credentials.from_authorized_user_file('mindsdb/integrations/handlers/google_fit_handler/token.json', SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_path, SCOPES)
                creds = flow.run_local_server(port=0)
            with open('mindsdb/integrations/handlers/google_fit_handler/token.json', 'w') as token:
                token.write(creds.to_json())
        self.api = build('fitness', 'v1', credentials=creds)

        self.is_connected = True
        return self.api

    def check_connection(self) -> StatusResponse:
        response = StatusResponse(False)

        try:
            self.connect()
            response.success = True

        except Exception as e:
            logger.error(f'Error connecting to Google Fit API: {e}!')
            response.error_message = e

        self.is_connected = response.success
        return response

    def retrieve_data(self, service, startTimeMillis, endTimeMillis, dataSourceId) -> dict:
        try:
            return service.users().dataset().aggregate(userId="me", body={
                "aggregateBy": [{
                    "dataTypeName": "com.google.step_count.delta",
                    "dataSourceId": dataSourceId
                }],
                "bucketByTime": {"durationMillis": 86400000},
                "startTimeMillis": startTimeMillis,
                "endTimeMillis": endTimeMillis
            }).execute()
        except HttpError:
            raise HttpError

    def native_query(self, query: str = None) -> Response:
        """Receive raw query and act upon it somehow.
        Args:
            query (Any): query in native format (str for sql databases,
            dict for mongo, api's json etc)
        Returns:
            HandlerResponse
        """
        ast = parse_sql(query)
        return self.query(ast)

    def get_steps(self, start_time_millis, end_time_millis) -> pd.DataFrame:
        steps = {}
        steps_data = self.retrieve_data(self.api, start_time_millis, end_time_millis, "derived:com.google.step_count.delta:com.google.android.gms:estimated_steps")
        for daily_step_data in steps_data['bucket']:
            local_date = datetime.fromtimestamp(int(daily_step_data['startTimeMillis']) / 1000,
                                                tz=pytz.timezone(str(get_localzone())))
            local_date_str = local_date.strftime(DATE_FORMAT)

            data_point = daily_step_data['dataset'][0]['point']
            if data_point:
                count = data_point[0]['value'][0]['intVal']
                data_source_id = data_point[0]['originDataSourceId']
                steps[local_date_str] = {'steps': count, 'originDataSourceId': data_source_id}
        ret = pd.DataFrame.from_dict(steps)
        ret = ret.T
        ret = ret.drop('originDataSourceId', axis=1)
        ret = ret.reset_index(drop=False)
        return ret

    def call_google_fit_api(self, method_name: str = None, params: dict = None) -> pd.DataFrame:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INSERT, DELETE, etc
        Returns:
            DataFrame
        """
        self.connect()
        if method_name == 'get_steps':
            val = self.get_steps(params['start_time'], params['end_time'])
            return val
        raise NotImplementedError('Method name {} not supported by Google Fit Handler'.format(method_name))
