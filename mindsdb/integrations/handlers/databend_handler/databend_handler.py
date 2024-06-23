from typing import Optional

import pandas as pd
from databend_sqlalchemy import connector

from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.libs.base import DatabaseHandler
from databend_sqlalchemy.databend_dialect import DatabendDialect

from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

logger = log.getLogger(__name__)


class DatabendHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Databend statements.
    """
    name = 'databend'

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
        self.dialect = 'databend'

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

        self.connection = connector.connect(
            f"https://{self.connection_data['user']}:{self.connection_data['password']}@{self.connection_data['host']}:{self.connection_data['port']}/{self.connection_data['database']}?secure=true"
        )
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
            logger.error(f'Error connecting to Databend, {e}!')
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
        cursor = connection.cursor()

        try:
            cursor.execute(query)
            result = cursor.fetchall()
            if result:
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=pd.DataFrame(
                        result,
                        columns=[x[0] for x in cursor.description]
                    )
                )
            else:
                connection.commit()
                response = Response(RESPONSE_TYPE.OK)
        except Exception as e:
            logger.error(f'Error running query: {query} on Databend!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

        cursor.close()
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
        renderer = SqlalchemyRender(DatabendDialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> StatusResponse:
        """
        Return list of entities that will be accessible as tables.
        Returns:
            HandlerResponse
        """

        query = f"""
            SHOW TABLES IN {self.connection_data["database"]}
        """
        result = self.native_query(query)
        df = result.data_frame

        df = df[[f'Tables_in_{self.connection_data["database"]}']]
        result.data_frame = df.rename(columns={f'Tables_in_{self.connection_data["database"]}': 'table_name'})

        return result
    
    def get_columns(self, table_name: str) -> StatusResponse:
        """
        Returns a list of entity columns.
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            HandlerResponse
        """

        query = f"""
            DESC {self.connection_data["database"]}.{table_name}
        """
        result = self.native_query(query)
        df = result.data_frame

        result.data_frame = df.rename(columns={'Field': 'column_name', 'Type': 'data_type', 'Null': 'is_nullable', 'Default': 'default_value', 'Extra': 'extra'})

        return result
