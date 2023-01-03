from typing import Optional
from collections import OrderedDict

import pandas as pd
import duckdb
import io
import requests

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


class InfluxDBHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Airtable statements.
    """

    name = 'influxdb'
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
        self.dialect = 'influxdb'
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
        
        url = f"{self.connection_data['influxdb_url']}/query"
        params = {
            ('db',f"{self.connection_data['influxdb_db_name']}"),
            ('q',f"{self.connection_data['influxdb_query']}" )
        }
        headers = {
            "Authorization": f"Token {self.connection_data['influxdb_token']}",
            "Accept": "application/csv",
        }
        response = requests.request("GET",url,params=params,headers=headers)
        
        if response.status_code == 200:
            self.connection = response

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
            log.error(f'Error connecting to influxdb portal {self.connection_data["influxdb_url"]}, {e}!')
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
        df = pd.read_csv(io.StringIO(connection.text))
        locals()[self.connection_data['influxdb_table_name']] = df
        
        try:            
            result = duckdb.query(query)
            if result:
                response = Response(RESPONSE_TYPE.TABLE,data_frame=result)                   
            else:
                response = Response(RESPONSE_TYPE.OK)

        except Exception as e:
            log.error(f'Error running query  {query} on influxdb handler')
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
                [self.connection_data['influxdb_table_name']],
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
        query = 'SELECT * FROM airSensors LIMIT 10'
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
    influxdb_url={
        'type': ARG_TYPE.STR,
        'description': 'URL of the hosted influxdb cloud instance'
    },
    influxdb_token={
        'type': ARG_TYPE.STR,
        'description': 'Authentication token for the hosted influxdb cloud instance'
    },
    influxdb_query={
        'type': ARG_TYPE.STR,
        'description': 'SQL Query you want to run on your hosted influxdb cloud instance'
    },
    influxdb_db_name={
        'type': ARG_TYPE.STR,
        'description': 'Database/Bucket name of the influxdb cloud instance'
    },
    influxdb_table_name={
        'type': ARG_TYPE.STR,
        'description': 'Table name of the influxdb cloud instance you want to query' 
    },

)

connection_args_example = OrderedDict(
    influxdb_url ='https://ap-southeast-2-1.aws.cloud2.influxdata.com',
    influxdb_token ='2KdXsJPE0yGpwxm6ybELC9-VjHYLN32QDTcNpRZUOVlgFJqJC7aUSLKcl26YwFP9_9jHqoih7FUWIy_zaIxfuw==',
    influxdb_query='SELECT * FROM airSensors Limit 10',
    influxdb_db_name='mindsdb',
    influxdb_table_name='airSensors'
)
