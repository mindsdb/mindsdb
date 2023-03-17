from typing import Optional

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse
from mindsdb.utilities import log
from mindsdb_sql import parse_sql

import jaydebeapi as jdbcconnector



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

    def connect(self) -> HandlerStatusResponse:
        """ Set up any connections required by the handler
        Should return output of check_connection() method after attempting
        connection. Should switch self.is_connected.
        Returns:
            HandlerStatusResponse
        """
        if self.is_connected is True:
            return self.connection
                
        user = self.connection_args.get('user')
        password = self.connection_args.get('password')
        jdbc_class = self.connection_args.get('jdbcClass', 'Altibase.jdbc.driver.AltibaseDriver')
        jar_location = self.connection_args.get('jdbcJarLocation')

        jdbc_url = f"jdbc:Altibase://{self.host}:{self.port}/{self.database};"

        try:
            if user and password and jar_location: 
                self.connection = jdbcconnector.connect(jclassname=jdbc_class, url=jdbc_url, driver_args=[user, password], jars=jar_location.split(","))
            elif user and password: 
                self.connection = jdbcconnector.connect(jclassname=jdbc_class, url=jdbc_url, driver_args=[user, password])
            elif jar_location: 
                self.connection = jdbcconnector.connect(jclassname=jdbc_class, url=jdbc_url, jars=jar_location.split(","))
            else:
                self.connection = jdbcconnector.connect(jdbc_class, jdbc_url)
        except Exception as e:
            log.logger.error(f"Error while connecting to {self.database}, {e}")

        return self.connection


    def disconnect():
        """
        It gracefully closes connections established in the connect method.
        """

    def check_connection():
        """
        It evaluates if the connection is alive and healthy. This method is called frequently.
        """

    def native_query():
        """
        It parses any native statement string and acts upon it (for example, raw SQL commands).
        """

    def query():
        """
        It takes a parsed SQL command in the form of an abstract syntax tree and executes it.
        """

    def get_tables():
        """
        It lists and returns all the available tables. Each handler decides what a table means for the underlying system when interacting with it from the data layer. Typically, these are actual tables.
        """
    def get_columns():
        """
        It returns columns of a table registered in the handler with the respective data type.
        """
