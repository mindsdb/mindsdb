from mindsdb_sql import parse_sql
import pandas as pd
from mendeley import Mendeley
from mindsdb.integrations.libs.api_handler import APIHandler, FuncParser
from mendeley.session import MendeleySession
from mindsdb.integrations.handlers.mendeley_handler.mendeley_tables import CatalogSearchTable 
from mindsdb.utilities import log
from typing import Dict
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

class MendeleyHandler(APIHandler): 

    def __init__(self, name, **kwargs): 

        """ constructor
        Args:
            name (str): the handler name
        """
        super().__init__(name)

        self.session = None
        self.is_connected = False
        
        self.session = self.connect()
    
        catalog_search_data = CatalogSearchTable(self)
        self.catalog_search_data = catalog_search_data 
        self._register_table('catalog_search_data', catalog_search_data)

    def connect(self) -> MendeleySession:
        """ The connect method sets up the connection required by the handler.
        In order establish a connection with Mendeley API one needs the client id and client secret that are
        created after registering the application at https://dev.mendeley.com/myapps.html . More information on the matter
        can be found at https://dev.mendeley.com/reference/topics/application_registration.html .
        In order to have access to Mendeley data we use "session".

        Returns:
        HandlerStatusResponse """

        if self.is_connected == True:
            return self.session

        mendeley = Mendeley(client_id=15253, client_secret="BxmSvbrRW5iYEIQR")
        auth = mendeley.start_client_credentials_flow()
        self.session = auth.authenticate()

        self.is_connected = True
        return self.session
    
    def check_connection(self) -> StatusResponse:

        """ The check_connection method checks the connection to the handler
        Returns:
            HandlerStatusResponse
        """
        response = StatusResponse(False)

        try:
            self.connect()
            response.success = True

        except Exception as e:
            log.logger.error(f'Error connecting to Mendeley: {e}!')
            response.error_message = str(e)

        self.is_connected = response.success
        return response

    def native_query(self, query_string: str):
        
        """The native_query method receives raw query and acts upon it.
            Args:
                query_string (str): query in native format
            Returns:
                HandlerResponse
        """
        ast = parse_sql(query_string, dialect="mindsdb")
        return self.query(ast)
    
    def get_authors(self,data):
        """The get_authors method receives the data - a specific document returned by the API, gets the names of the authors 
            and combines them in a string, so as to allow the use of DataFrame.
            Args:
                data (CatalogDocument): document returned by API
            Returns:
                authors string
        """
        authors = ""
        sum = 0
        if data.authors!=None:
            for x in data.authors:
                if sum + 1 == len(data.authors) and x.first_name!=None and x.last_name!=None  :
                    authors = authors + x.first_name + " " + x.last_name
                else:
                    if x.first_name!=None and x.last_name!=None:
                        authors = authors + x.first_name + " " + x.last_name + ", "
                        sum = sum + 1
        return authors

    def get_keywords(self,data):
        """The get_keywords method receives the data-a specific document returned by the API, gets the specified keywords 
            and combines them in a string, so as to allow the use of DataFrame.
            Args:
                data (CatalogDocument) : document returned by the API
            Returns:
                keywords string
        """
        keywords = ""
        sum = 0
        if data.keywords!=None:
            for x in data.keywords:
                if sum + 1 == len(data.keywords):
                    keywords = keywords  + x + " "
                else:
                    if x!=None :
                        keywords = keywords + x + ", "
                        sum = sum + 1
        return keywords
