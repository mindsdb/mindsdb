from typing import Optional

import pandas as pd
from elasticsearch import Elasticsearch

from elasticsearch.exceptions import ConnectionError, AuthenticationException

from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from es.elastic.sqlalchemy import ESDialect
from mindsdb.integrations.libs.base import DatabaseHandler

from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

logger = log.getLogger(__name__)

class ElasticsearchHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Airtable statements.
    """

    name = 'elasticsearch'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> Elasticsearch:
        """
        Set up the connection required by the handler.
        Returns:
            HandlerStatusResponse
        """

        if self.is_connected is True:
            return self.connection
        
        # Mandatory connection parameters.
        if not any(key in self.connection_data for key in ['hosts', 'cloud_id']):
            raise ValueError('Either hosts or cloud_id must be provided.')
        
        config = {}

        if self.connection_data['hosts']:
            config['hosts'] = self.connection_data['hosts'].split(',')

        if self.connection_data['cloud_id']:
            config['cloud_id'] = self.connection_data['cloud_id']

        # Username and password are optional, but if one is provided, both must be provided.
        username = self.connection_data.get('username')
        password = self.connection_data.get('password')
        if username and not password:
            raise ValueError('Password must be provided along with username.')
        if password and not username:
            raise ValueError('Username must be provided along with password.')

        if username and password:
            config['basic_auth'] = (username, password)

        try:
            self.connection = Elasticsearch(
                **config,
            )
            self.is_connected = True
            return self.connection
        except ConnectionError as conn_error:
            logger.error(f'Connection error when connecting to Elasticsearch: {conn_error}')
            raise conn_error
        except AuthenticationException as auth_error:
            logger.error(f'Authentication error when connecting to Elasticsearch: {auth_error}')
            raise auth_error
        except Exception as e:
            logger.error(f'Unknown error connecting to Elasticsearch: {e}')
            raise e

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
            logger.error(f'Error connecting to Elasticsearch {self.connection_data["hosts"]}, {e}!')
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

        try:
            response = connection.sql.query(body={'query': query})
            records = response['rows']
            columns = response['columns']

            new_records = True
            while new_records:
                try:
                    if response['cursor']:
                        response = connection.sql.query(body={'query': query, 'cursor': response['cursor']})

                        new_records = response['rows']
                        records = records + new_records
                except Exception as e:
                    new_records = False

            if records:
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=pd.DataFrame(
                        records,
                        columns=[column['name'] for column in columns]
                    )
                )
        except Exception as e:
            logger.error(f'Error running query: {query} on {self.connection_data["hosts"]}!')
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
                of query: SELECT, INSERT, DELETE, etc
        Returns:
            HandlerResponse
        """

        renderer = SqlalchemyRender(ESDialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> StatusResponse:
        """
        Return list of indexes that will be accessible as tables.
        Returns:
            HandlerResponse
        """

        query = """
            SHOW TABLES
        """
        result = self.native_query(query)
        df = result.data_frame
        df = df.drop(['type', 'type'], axis=1)
        result.data_frame = df.rename(columns={'name': 'table_name'})

        return result

    def get_columns(self, table_name: str) -> StatusResponse:
        """
        Returns a list of entity columns.
        Args:
            table_name (str): name of one of indexes returned by self.get_tables()
        Returns:
            HandlerResponse
        """

        query = f"""
            DESCRIBE {table_name}
        """
        result = self.native_query(query)
        df = result.data_frame
        df = df.drop('mapping', axis=1)
        result.data_frame = df.rename(columns={'column': 'column_name', 'type': 'data_type'})

        return result
