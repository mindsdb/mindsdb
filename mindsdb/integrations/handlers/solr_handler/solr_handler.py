from typing import Optional
import pandas as pd

from sqlalchemy import create_engine

from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser.ast.base import ASTNode

from mindsdb.utilities import log
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)


logger = log.getLogger(__name__)


class SolrHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Solr SQL statements.
    """

    name = 'solr'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = 'solr'

        if ('host' not in connection_data) or ('port' not in connection_data) or ('collection' not in connection_data):
            raise Exception("The host, port and collection parameter should be provided!")

        optional_parameters = ['use_ssl', 'username', 'password']
        for parameter in optional_parameters:
            if parameter not in connection_data:
                connection_data[parameter] = None

        if connection_data.get('use_ssl', False):
            connection_data['use_ssl'] = True
        else:
            connection_data['use_ssl'] = False

        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self):
        """
        Set up the connection required by the handler.
        Returns:
            HandlerStatusResponse
        """
        if self.is_connected is True:
            return self.connection

        config = {
            'username': self.connection_data.get('username'),
            'password': self.connection_data.get('password'),
            'host': self.connection_data.get('host'),
            'port': self.connection_data.get('port'),
            'server_path': self.connection_data.get('server_path', 'solr'),
            'collection': self.connection_data.get('collection'),
            'use_ssl': self.connection_data.get('use_ssl')
        }

        connection = create_engine("solr://{username}:{password}@{host}:{port}/{server_path}/{collection}/sql?use_ssl={use_ssl}".format(**config))
        self.is_connected = True
        self.connection = connection.connect()
        return self.connection

    def disconnect(self):
        """
        Close any existing connections.
        """
        if self.is_connected is False:
            return
        self.connection.close()
        self.is_connected = False
        return

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the Solr database
        Returns:
            HandlerStatusResponse
        """

        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            response.success = True
        except Exception as e:
            logger.error(f'Error connecting to Solr {self.connection_data["host"]}, {e}!')
            response.error_message = str(e)

        if response.success is True and need_to_close:
            self.disconnect()
        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query: str) -> Response:
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
            result = connection.execute(query)
            columns = list(result.keys())
            if result:
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    pd.DataFrame(
                        result,
                        columns=columns
                    )
                )
            else:
                response = Response(RESPONSE_TYPE.OK)

        except Exception as e:
            logger.error(f'Error running query: {query} on {self.connection_data["host"]}!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement.
        """
        return self.native_query(query.to_string())

    def get_tables(self) -> Response:
        """
        Get a list with all of the tables in Solr
        """
        result = {}
        result['data_frame'] = pd.DataFrame([self.connection_data.get('collection')])
        df = result.data_frame
        result.data_frame = df.rename(columns={df.columns[0]: 'table_name'})
        return result

    def get_columns(self, table_name) -> Response:
        """
        Show details about the table
        """
        q = f"select * from {table_name} limit 1"
        result = self.native_query(q)
        df = pd.DataFrame([[col] for col in result.data_frame.columns])
        result.data_frame = df.rename(columns={df.columns[0]: 'column_name'})
        return result
