import os
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import pandas as pd
from pandas import DataFrame
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from .google_books_tables import BookshelvesTable, VolumesTable
from mindsdb.integrations.libs.api_handler import APIHandler, FuncParser
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class GoogleBooksHandler(APIHandler):
    """
        A class for handling connections and interactions with the Google Books API.
    """
    name = 'google_books'

    def __init__(self, name: str, **kwargs):
        """
        Initialize the Google Books API handler.
        Args:
            name (str): name of the handler
            kwargs (dict): additional arguments
        """
        super().__init__(name)
        self.token = None
        self.service = None
        self.connection_data = kwargs.get('connection_data', {})
        self.credentials_file = self.connection_data.get('credentials', None)
        self.credentials = None
        self.scopes = ['https://www.googleapis.com/auth/books']
        self.is_connected = False
        self.connection = None
        bookshelves = BookshelvesTable(self)
        self.bookshelves = bookshelves
        self._register_table('bookshelves', bookshelves)
        volumes = VolumesTable(self)
        self.volumes = volumes
        self._register_table('volumes', volumes)

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
            if os.path.exists('token_books.json'):
                self.credentials = Credentials.from_authorized_user_file('token_books.json', self.scopes)
            if not self.credentials or not self.credentials.valid:
                if self.credentials and self.credentials.expired and self.credentials.refresh_token:
                    self.credentials.refresh(Request())
                else:
                    self.credentials = service_account.Credentials.from_service_account_file(
                        self.credentials_file, scopes=self.scopes)
            # Save the credentials for the next run
            with open('token_books.json', 'w') as token:
                token.write(self.credentials.to_json())
            self.service = build('books', 'v1', credentials=self.credentials)
        return self.service

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler
        Returns:
            HandlerStatusResponse
        """

        response = StatusResponse(False)

        try:
            self.connect()
            response.success = True
        except Exception as e:
            logger.error(f'Error connecting to Google Books API: {e}!')
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

    def get_bookshelves(self, params: dict = None) -> DataFrame:
        """
        Get bookshelf from Google Books API
        Args:
            params (dict): query parameters
        Returns:
            DataFrame
        """
        service = self.connect()
        minShelf = None
        maxShelf = None
        if params['shelf']:
            shelf = int(params['shelf'])
            if params['source']:
                df = service.mylibrary().bookshelves().get(shelf=shelf, userid=params['userid'],
                                                           source=params['source']).execute()
            else:
                df = service.mylibrary().bookshelves().get(shelf=shelf, userid=params['userid']).execute()

            df = pd.DataFrame(df, columns=self.bookshelves.get_columns())
            return df
        elif not params['minShelf'] and params['maxShelf']:
            minShelf = int(params['maxShelf']) - 10
            maxShelf = int(params['maxShelf'])
        elif not params['maxShelf'] and params['minShelf']:
            minShelf = int(params['minShelf'])
            maxShelf = int(params['minShelf']) + 10
        elif params['maxShelf'] and params['minShelf']:
            minShelf = int(params['minShelf'])
            maxShelf = int(params['maxShelf'])

        args = {
            key: value for key, value in params.items() if key not in ['minShelf', 'maxShelf'] and value is not None
        }
        bookshelves = service.bookshelves().list(userid=params['userid'], **args).execute()

        df = pd.DataFrame(bookshelves['items'], columns=self.bookshelves.get_columns())
        if minShelf is not None or maxShelf is not None:
            # Drop bookshelves that are not in the id range
            df = df.drop(df[(df['id'] < minShelf) | (df['id'] > maxShelf)].index)

        return df

    def get_volumes(self, params: dict = None) -> DataFrame:
        """
        Get volumes from Google Books API
        Args:
            params (dict): query parameters
        Returns:
            DataFrame
        """
        service = self.connect()
        args = {
            key: value for key, value in params.items() if value is not None
        }
        volumes = service.volumes().list(**args).execute()
        df = pd.DataFrame(volumes['items'], columns=self.volumes.get_columns())
        return df

    def call_application_api(self, method_name: str = None, params: dict = None) -> DataFrame:
        """
        Call Google Books API and map the data to pandas DataFrame
        Args:
            method_name (str): method name
            params (dict): query parameters
        Returns:
            DataFrame
        """
        if method_name == 'get_bookshelves':
            return self.get_bookshelves(params)
        elif method_name == 'get_volumes':
            return self.get_volumes(params)
        else:
            raise NotImplementedError(f'Unknown method {method_name}')
