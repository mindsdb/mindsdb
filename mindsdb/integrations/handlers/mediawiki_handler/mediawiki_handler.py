from mediawikiapi import MediaWikiAPI

from mindsdb.integrations.handlers.mediawiki_handler.mediawiki_tables import PagesTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
import pandas as pd
from mindsdb.utilities import log
from mindsdb_sql import parse_sql

import requests

class MediaWikiHandler(APIHandler):
    """
    The MediaWiki handler implementation.
    """

    name = 'mediawiki'

    #Get Login Token Params
    LOGIN_PARAMS = {
        'action': 'query',
        'meta': 'tokens',
        'type': 'login',
        'format': 'json'
    }

    #Get CSRF Token Params
    CSRF_PARAMS = {
        'action': 'query',
        'meta': 'tokens',
        'format': 'json'
    }

    def __init__(self, name: str, **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)

        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

        pages_data = PagesTable(self)
        self._register_table("pages", pages_data)

    def connect(self):
        """
        Set up the connection required by the handler.
        Returns
        -------
        StatusResponse
            connection object
        """
        if self.is_connected is True:
            return self.connection

        self.connection = MediaWikiAPI()

        #Completely unsure how we get the username and password
        #
        #
        user, pw = None
        
        self.login(user, pw)
        self.is_connected = True

        return self.connection

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """

        response = StatusResponse(False)

        try:
            self.connect()
            response.success = True
        except Exception as e:
            log.logger.error(f'Error connecting to MediaWiki!')
            response.error_message = str(e)

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
    
    def call_application_api(self, method_name:str = None, params:dict = None) -> pd.DataFrame:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INSERT, DELETE, etc
        Returns:
            pd.DataFrame
        """
        #Connect and Login
        self.connect()
        #Get csrf Token
        csrf = self.get_csrf()
        if csrf is not None:
            params['csrf'] = csrf
            params.update({'action': method_name})
            #post changes
            response = self.call_api('POST', params=params)
            df = pd.DataFrame(response)
            return df
        return None

    # 
    def call_api(self, method, **params):
        try:
            if method == 'GET':
                response = requests.get(self.API_URL, params=params)
            elif method == 'POST':
                response = requests.post(self.API_URL, data=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            log.logger.error(f'API call error: { e }')
            return None

    #Useful for Insert and Delete Issues
    def login(self, user, pwd):
        #Fetch login token
        params = self.LOGIN_PARAMS.copy()
        params.update({'lgname': user, 'lgpassword': pwd})
        response = self.call_api('GET', params)
        #Use login token to login
        if response:
            login_token = response.json()['query']['tokens']['logintoken']
            params.update({'lgtoken': login_token, 'action': 'login'})
            self.call_api('POST', data=params)

    #Useful for Insert and Delete Issues
    def get_csrf(self, ):
        response = self.call_api('GET', self.CSRF_PARAMS)
        if response:      
            return response.json()['query']['tokens']['csrftoken']
        return None

