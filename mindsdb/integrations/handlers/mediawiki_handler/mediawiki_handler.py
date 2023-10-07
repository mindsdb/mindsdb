from mediawikiapi import MediaWikiAPI

from mindsdb.integrations.handlers.mediawiki_handler.mediawiki_tables import PagesTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)

from mindsdb.utilities import log
from mindsdb_sql import parse_sql

from typing import Optional
import pandas as pd
import requests

class MediaWikiHandler(APIHandler):
    """
    The MediaWiki handler implementation.
    """

    name = 'mediawiki'
    #How do we want to get these?
    username = ''
    pwd = ''

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

    def __init__(self, name: str, username: Optional[str] = None, password: Optional[str] = None, **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        self.kwargs= kwargs
        self.API_URL = 'https://www.mediawiki.org/w/api.php'
        self.username = username if username else None #How do we get these?
        self.password = password if password else None#How do we get these?
        self.isLogged= False
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
            log.logger.error('Error connecting to MediaWiki!')
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
    
    def call_application_api(self, operation:str, params:dict) -> pd.DataFrame:
        """
        Perform specified auth-required operation 

        Connects and login to API, fetch CSRF token, then performs specified operation

        Args:
            operation (str, optional)
            params
        """
        try:
            if not self.is_connected:    
                self.connect()
            if not self.isLogged:
                self.login(self.username, self.password)
        
            csrf = self.get_csrf()
            if csrf is not None:
                params['csrf'] = csrf
                params.update({'action': operation})
                response = requests.post(self.API_URL, params=params, timeout=5)
                df = pd.DataFrame(response.json())
                return df
        except Exception as e:
            log.logger.error(f"Error during call_application_api: {e}")
            return None 

    #Useful for Insert and Delete Issues
    def login(self, user:str, pwd:str) -> None:
        """
        Authenticate a user with the MediaWiki API.

        Args:
            user (str): The username.
            pwd (str): The password.

        Returns:
            None
        """
        params = self.LOGIN_PARAMS.copy()
        params.update({'lgname': user, 'lgpassword': pwd})
        response = requests.get(self.API_URL, params)
        if response:
            login_token = response.json()['query']['tokens']['logintoken']
            params.update({'lgtoken': login_token, 'action': 'login'})
            login_response = requests.post(self.API_URL, data=params, timeout=5)
            if login_response.status_code == 200:
                self.isLogged = True
            else:
                self.isLogged = False
                raise Exception("Login failed with status code: " + str(login_response.status_code))
            
        
    #Useful for Insert and Delete Issues
    def get_csrf(self) -> Optional[str]:
        """
        Fetch a CSRF token from api

        Returns:
            str: CSRF Token
        """
        response = requests.get(self.API_URL, self.CSRF_PARAMS)
        if response:      
            return response.json()['query']['tokens']['csrftoken']
        return None

