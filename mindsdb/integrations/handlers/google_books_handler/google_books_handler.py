import json
from urllib.parse import urlencode
import requests
import pandas as pd
from pandas import DataFrame

from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from .google_books_tables import BookshelvesTable, VolumesTable
from mindsdb.integrations.libs.api_handler import APIHandler, FuncParser
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)
from mindsdb.utilities import log


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
        self.connection_data = kwargs.get('connection_data', {})
        self.api_key = self.connection_data.get('api_key')
        self.is_connected = False
        self.connection = None
        bookshelves = BookshelvesTable(self)
        self.bookshelves = bookshelves
        self._register_table('bookshelves', bookshelves)
        volumes = VolumesTable(self)
        self.volumes = volumes
        self._register_table('volumes', volumes)

    def connect(self) -> StatusResponse:
        """
        Set up any connections required by the handler
        Should return output of check_connection() method after attempting
        connection. Should switch self.is_connected.
        Returns:
            HandlerStatusResponse
        """
        if self.is_connected is True:
            return self.connection

        self.connection = self.check_connection()
        return self.connection

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler
        Returns:
            HandlerStatusResponse
        """
        response = StatusResponse(False)

        try:
            if self.api_key is None:
                raise Exception('Missing API key')
            response = requests.get(
                'https://www.googleapis.com/books/v1/volumes'
                '?q=flowers+inauthor:keyes&key=' + self.api_key)
            if response.status_code != 200:
                raise Exception(f'Error connecting to Google Books API: {response.text}')
            response.success = True
        except Exception as e:
            log.logger.error(f'Error connecting to Google Books API: {e}!')
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
        df = pd.DataFrame(columns=self.bookshelves.get_columns())
        minShelf = None
        maxShelf = None
        if params['shelf']:
            # Build the request URL
            base_url = f"https://www.googleapis.com/books/v1/users/{params['userId']}/bookshelves/{params['shelf']}"
            url = base_url + "?" + urlencode(params) + f"&key={self.api_key}"
            if not (params['source'] and params['fields']):
                url = base_url + f"?key={self.api_key}"
            response = response = requests.get(url)
            # Check if the request was successful (i.e., status code 200)
            if response.status_code == 200:
                # Parse the response JSON into a Python dictionary
                data = json.loads(response.text)
                df = pd.DataFrame(data)
            else:
                log.logger.error(f"Error retrieving bookshelf information: {response.status_code}")
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

        # Build the request URL
        base_url = f"https://www.googleapis.com/books/v1/users/{params['userId']}/bookshelves"
        url = base_url + "?" + urlencode(params) + f"&key={self.api_key}"
        if not (params['source'] and params['fields']):
            url = base_url + f"?key={self.api_key}"

        response = requests.get(url)
        if response.status_code == 200:
            data = json.loads(response.text)
            df = pd.DataFrame(data['items'])
            if minShelf is not None or maxShelf is not None:
                # Drop bookshelves that are not in the id range
                df = df.drop(df[(df['id'] < minShelf) | (df['id'] > maxShelf)].index)
        else:
            log.logger.error(f"Error retrieving bookshelf information: {response.status_code}")

        return df

    def get_volumes(self, params: dict = None) -> DataFrame:
        """
        Get volumes from Google Books API
        Args:
            params (dict): query parameters
        Returns:
            DataFrame
        """
        df = pd.DataFrame(columns=self.volumes.get_columns())
        # Build the request URL
        base_url = f"https://www.googleapis.com/books/v1/volumes"
        url = base_url + "?" + urlencode(params) + f"&key={self.api_key}"
        response = requests.get(url)
        if response.status_code == 200:
            data = json.loads(response.text)
            df = pd.DataFrame(data['items'])
        else:
            log.logger.error(f"Error retrieving volume information: {response.status_code}")

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
