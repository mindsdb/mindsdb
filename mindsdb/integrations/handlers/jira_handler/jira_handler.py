from typing import Optional
from collections import OrderedDict

import pandas as pd
import duckdb
import json
from atlassian import Jira

from mindsdb_sql import parse_sql
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.utilities.log import get_log
from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

log = get_log()


class JiraHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Airtable statements.
    """

    name = 'jira'
    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = 'jira'
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

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
        
        self.connection = Jira(
            url=self.connection_data['jira_url'],
            username=self.connection_data['user_id'],
            password=self.connection_data['api_key'],
            cloud=True)
        
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
            log.error(f'Error connecting to Jira portal {self.connection_data["jira_url"]}, {e}!')
            response.error_message = str(e)
        finally:
            if response.success is True and need_to_close:
                self.disconnect()
            if response.success is False and self.is_connected is True:
                self.is_connected = False

        return response

    def native_query(self, query: str) -> StatusResponse:
        """
        Receive raw query and act upon it somehow.
        Args:
            query (str): query in native format
        Returns:
            HandlerResponse
        """

        need_to_close = self.is_connected is False

        connection = self.connect()
        results = connection.jql(self.connection_data['jira_query'])
        df = pd.json_normalize(results["issues"])
        fields_name= ["key", "fields.summary","fields.status.name", "fields.reporter.name","fields.assignee.name","fields.priority.name"]
        locals()[self.connection_data['table_name']] = df[fields_name]
        try:            
            result = duckdb.query(query)
            if result:
                response = Response(RESPONSE_TYPE.TABLE,data_frame=result.df())                   
            else:
                response = Response(RESPONSE_TYPE.OK)

        except Exception as e:
            log.error(f'Error running query  {query} on jira handler')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )
        
        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> StatusResponse:
        """
        Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INTSERT, DELETE, etc
        Returns:
            HandlerResponse
        """

        return self.native_query(query.to_string())

    def get_tables(self) -> StatusResponse:
        """
        Return list of entities that will be accessible as tables.
        Returns:
            HandlerResponse
        """

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                [self.connection_data['table_name']],
                columns=['table_name']
            )
        )

        return response

    def get_columns(self) -> StatusResponse:
        """
        Returns a list of entity columns.
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            HandlerResponse
        """
        query = 'SELECT * FROM '+ str(self.connection_data['table_name'])+ ' LIMIT 10'
        result = self.native_query(query)

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                {
                    'column_name': result.data_frame.columns,
                    'data_type': result.data_frame.dtypes
                }
            )
        )

        return response


connection_args = OrderedDict(
    table_name={
        'type': ARG_TYPE.STR,
        'description': 'Sample Jira table name.'
    },
    jira_url={
        'type': ARG_TYPE.STR,
        'description': 'Jira  url'
    },
    user_id={
        'type': ARG_TYPE.STR,
        'description': 'Jira User ID.'
    },
    api_key={
        'type': ARG_TYPE.STR,
        'description': 'API key for the Jira API.'
    },
    jira_query={
        'type': ARG_TYPE.STR,
        'description': 'Jira Search Query '
    }
   
)

connection_args_example = OrderedDict(
    table_name ='project',
    jira_url ='https://jira.linuxfoundation.org/',
    user_id='balaceg',
    api_key='4Rhq&Ehd#KV4an!',
    jira_query = 'project = RELENG and status = In Progress'
)
