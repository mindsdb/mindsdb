from mindsdb.integrations.handlers.jira_handler.jira_table import JiraProjectsTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.utilities import log
from mindsdb_sql import parse_sql

from atlassian import Jira
from typing import Optional
import requests


logger = log.getLogger(__name__)

class JiraHandler(APIHandler):
    """
    This handler handles connection and execution of the Airtable statements.
    """


    def __init__(self, name=None, **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = kwargs.get("connection_data", {})

        self.parser = parse_sql
        self.dialect = 'jira'
        self.kwargs = kwargs
        self.connection = None
        self.is_connected = False


        jira_projects_data = JiraProjectsTable(self)
        self._register_table("project", jira_projects_data)

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> StatusResponse:
        """
        Set up the connection required by the handler.
        Returns:
            HandlerStatusResponse
        """

        if self.is_connected is True:
            return self.connection
        
        s = requests.Session()
        s.headers['Authorization'] =  f"Bearer {self.connection_data['jira_api_token']}"

        self.connection = Jira(url= self.connection_data['jira_url'], session=s)
        self.is_connected = True


        return self.connection

    def disconnect(self):
        """
        Close any existing connections.
        """

        if self.is_connected is False:
            return

        self.connection.close()
        self.is_connected = False
        return self.is_connected

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """

        response = StatusResponse(False)
        need_to_close = self.is_connected is False
        
        try:
            self.connect()
            response.success = True
        except Exception as e:
            logger.error(f"Error connecting to Jira API: {e}!")
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
   
    def construct_jql(self):
        """construct jql & returns it to JiraProjectsTable class
        Returns
        -------
        Str
        """
        return 'project = ' + str(self.connection_data['project'])
