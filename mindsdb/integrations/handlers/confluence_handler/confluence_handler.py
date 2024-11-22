from typing import Optional, Union

import requests
from atlassian import Confluence
from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.handlers.confluence_handler.confluence_table import (
    ConfluencePagesTable,
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb_sql_parser import parse_sql
from mindsdb.utilities import log

from atlassian import Confluence
from typing import Optional
import requests

logger = log.getLogger(__name__)

class ConfluenceHandler(APIHandler):
    """Confluence handler implementation"""

    def __init__(self, name=None, **kwargs):
        """Initialize the Confluence handler.
        Parameters
        ----------
        name : str
            name of a handler instance
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})

        self.parser = parse_sql
        self.dialect = 'confluence'
        self.connection_data = connection_data
        self.kwargs = kwargs
        self.connection = None
        self.is_connected = False

        confluence_pages_data = ConfluencePagesTable(self)
        self._register_table("pages", confluence_pages_data)

    def connect(self):
        """Set up the connection required by the handler.
        Returns
        -------
        StatusResponse
            connection object
        """
        if self.is_connected is True:
            return self.connection
        
        if not all(key in self.connection_data and self.connection_data.get(key) for key in ['url', 'username', 'password']):
            raise ValueError('Required parameters (url, username, password) must be provided and should not be empty.')
        
        conf = Confluence(
            url=self.connection_data.get('url'),
            username=self.connection_data.get('username'),
            password=self.connection_data.get('password'),
        )
        self.connection = conf
        self.is_connected = True
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
            logger.error(f"Error connecting to Confluence API: {e}!")
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
        ast = parse_sql(query)
        return self.query(ast)
