from typing import Optional, Union

import requests
from atlassian import Confluence
from mindsdb_sql import parse_sql

from mindsdb.integrations.handlers.confluence_handler.confluence_table import (
    ConfluenceSpacesTable,
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.utilities.log import get_log

logger = get_log("integrations.confluence_handler")


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

        confluence_pages_data = ConfluenceSpacesTable(self)
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
        ast = parse_sql(query, dialect="mindsdb")
        return self.query(ast)

    def create_page(self, space: str, title: str, body: str) -> StatusResponse:
        """Create a new page.
        Parameters
        ----------
        space : str
            space key
        title : str
            page title
        body : str
            page body
        Returns
        -------
        StatusResponse
            Request status
        """
        response = StatusResponse(False)
        try:
            conn = self.connect()
            conn.create_page(space, title, body)
            response.success = True
        except Exception as e:
            logger.error(f"Error creating page: {e}!")
            response.error_message = e
        return response

    def modify_page(
        self, page_id: Union[str, int], title: str, body: str
    ) -> StatusResponse:
        """Modify an existing page.
        Parameters
        ----------
        page_id : str
            page id
        title : str
            page title
        body : str
            page body
        Returns
        -------
        StatusResponse
            Request status
        """
        response = StatusResponse(False)
        try:
            conn = self.connect()
            conn.update_page(page_id, title, body)
            response.success = True
        except Exception as e:
            logger.error(f"Error modifying page: {e}!")
            response.error_message = e
        return response

    def delete_page(self, page_id: Union[str, int]) -> StatusResponse:
        """Delete a page based on its id.
        Parameters
        ----------
        content_id : str
            content id
        Returns
        -------
        StatusResponse
            Request status
        """
        response = StatusResponse(False)
        try:
            conn = self.connect()
            conn.remove_content(content_id=page_id)
            response.success = True
        except Exception as e:
            logger.error(f"Error deleting page: {e}!")
            response.error_message = e
        return response

    def get_content(self, content_id: Union[str, int]) -> dict:
        """Get a piece of Content based on the id.
        Parameters
        ----------
        content_id : str
            page id
        Returns
        -------
        dict
            Content object
        """
        try:
            conn = self.connect()
            return conn.get_page_by_id(content_id)
        except Exception as e:
            logger.error(f"Error getting content: {e}!")
