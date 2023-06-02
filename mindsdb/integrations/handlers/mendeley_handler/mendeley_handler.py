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