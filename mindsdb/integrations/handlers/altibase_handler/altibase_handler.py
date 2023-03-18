from typing import Any, Optional

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.utilities import log
from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast.base import ASTNode

import jaydebeapi as jdbcconnector
import pandas as pd



class AltibaseHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Altibase statements.
    """

    name = 'altibase'

    def __init__(self, name: str, connection_data: Optional[dict]):
        """ constructor
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
        """
        super().__init__(name)
        
        self.parser = parse_sql

        self.connection_args = connection_data
        self.database = self.connection_args.get('database')
        self.host = self.connection_args.get('host')
        self.port = self.connection_args.get('port')
        
        self.connection = None
        self.is_connected = False

    def connect(self):
        """ Set up any connections required by the handler
        Should return output of check_connection() method after attempting connection.
        Should switch self.is_connected.
        Returns:
            connection
        """
        if self.is_connected is True:
            return self.connection
                
        user = self.connection_args.get('user')
        password = self.connection_args.get('password')
        jar_location = self.connection_args.get('jdbcJarLocation')

        jdbc_class = self.connection_args.get('jdbcClass', 'Altibase.jdbc.driver.AltibaseDriver')
        jdbc_url = f"jdbc:Altibase://{self.host}:{self.port}/{self.database}"

        try:
            if user and password and jar_location: 
                connection = jdbcconnector.connect(jclassname=jdbc_class, url=jdbc_url, driver_args=[user, password], jars=jar_location.split(","))
            elif user and password: 
                connection = jdbcconnector.connect(jclassname=jdbc_class, url=jdbc_url, driver_args=[user, password])
            elif jar_location: 
                connection = jdbcconnector.connect(jclassname=jdbc_class, url=jdbc_url, jars=jar_location.split(","))
            else:
                connection = jdbcconnector.connect(jclassname=jdbc_class, url=jdbc_url)
        except Exception as e:
            log.logger.error(f"Error while connecting to {self.database}, {e}")
        
        self.is_connected = True
        self.connection = connection
        return self.connection


    def disconnect(self):
        """ Close any existing connections
        Should switch self.is_connected.
        """
        if self.is_connected is False:
            return
        try:
            self.connection.close()
            self.is_connected = False
        except Exception as e:
            log.logger.error(f"Error while disconnecting to {self.database}, {e}")
        return 

    def check_connection(self) -> StatusResponse:
        """ Check connection to the handler
        Returns:
            HandlerStatusResponse
        """
        responseCode = StatusResponse(success=False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            responseCode.success = True
        except Exception as e:
            log.logger.error(f'Error connecting to database {self.database}, {e}!')
            responseCode.error_message = str(e)
        finally:
            if responseCode.success is True and need_to_close:
                self.disconnect()
            if responseCode.success is False and self.is_connected is True:
                self.is_connected = False

        return responseCode

    def native_query(self, query: str) -> Response:
        """Receive raw query and act upon it somehow.
        Args:
            query (str): query in native format
        Returns:
            HandlerResponse
        """
        need_to_close = self.is_connected is False
        connection = self.connect()
        with connection.cursor() as cur:
            try:
                cur.execute(query)
                if cur.description:
                    result = cur.fetchall() 
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        data_frame=pd.DataFrame(
                            result,
                            columns=[x[0] for x in cur.description]
                        )
                    )
                else:
                    response = Response(RESPONSE_TYPE.OK)
                connection.commit()
            except Exception as e:
                log.logger.error(f'Error running query: {query} on {self.database}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(e)
                )
                connection.rollback()

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind of query: SELECT, INSERT, DELETE, etc
        Returns:
            HandlerResponse
        """
        if isinstance(query, ASTNode):
            query_str = query.to_string()
        else:
            query_str = str(query)

        return self.native_query(query_str)

    def get_tables():
        """
        It lists and returns all the available tables. Each handler decides what a table means for the underlying system when interacting with it from the data layer. Typically, these are actual tables.
        """
    def get_columns():
        """
        It returns columns of a table registered in the handler with the respective data type.
        """
