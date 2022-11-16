import psycopg2
from pandas import DataFrame
from typing import Optional
from collections import OrderedDict
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)


class YugabyteHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the YugabyteSQL statements.
    """
    name = 'yugabyte'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        super().__init__(name)
        self.parser = parse_sql
        self.connection_data = connection_data
        self.dialect = 'postgresql'
        self.database = connection_data['database']
        self.renderer = SqlalchemyRender('postgres')

        self.connection = None
        self.is_connected = False


    def connect(self):
        """
        Handles the connection to a YugabyteSQL database insance.
        """
        if self.is_connected is True:
            return self.connection
        
        args={
            "dbname": self.database,
            "host": self.connection_data['host'],
            "port": self.connection_data['port'],
            "user": self.connection_data['user'],
            "password": self.connection_data['password'],
        }



        connection = psycopg2.connect(**args, connect_timeout=10)

        self.is_connected = True
        self.connection = connection
        return self.connection

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the PostgreSQL database
        :return: success status and error message if error occurs
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()
            with connection.cursor() as cur:
                cur.execute('select 1;')
            response.success = True
        except psycopg2.Error as e:
            log.logger.error(f'Error connecting to PostgreSQL {self.database}, {e}!')
            response.error_message = e

        if response.success is True and need_to_close:
            self.disconnect()
        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query: str) -> StatusResponse:
        """Receive raw query and act upon it somehow.
        Args:
            query (Any): query in native format (str for sql databases,
                dict for mongo, etc)
        Returns:
            HandlerResponse
        """
        need_to_close = self.is_connected is False
        conn = self.connect()
        with conn.cursor() as cur:
            try:
                cur.execute(query)
                   
                if cur.rowcount >0 and query.upper().startswith('SELECT') :
                    result = cur.fetchall() 
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        data_frame=DataFrame(
                            result,
                            columns=[x[0] for x in cur.description]
                        )
                    )
                else:
                    response = Response(RESPONSE_TYPE.OK)
                self.connection.commit()
            except Exception as e:
                log.logger.error(f'Error running query: {query} on {self.database}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(e)
                )
                self.connection.rollback()

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement with eliminated rows that dont satisfy the WHERE condition
        """
        query_str = self.renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        List all tabels in PostgreSQL without the system tables information_schema and pg_catalog
        """

        query = """SELECT table_schema,table_name,table_type FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog') and table_type in ('BASE TABLE', 'VIEW')"""
        return self.query(query)

    def get_columns(self, table_name):
        query = f"""SELECT column_name as "Field", data_type as "Type" FROM information_schema.columns WHERE table_name = '{table_name}'"""
        return self.query(query)



connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the YugabyteDB server/database.'
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the YugabyteDB server.'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'The password to authenticate the user with the YugabyteDB server.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'Specify port to connect YugabyteDB server'
    }, 
    database={
        'type': ARG_TYPE.STR,
        'description': 'Specify database name  to connect YugabyteDB server'
    },


)

connection_args_example = OrderedDict(
    host='127.0.0.1',
    port=5433,
    password='',
    user='admin',
    database='yugabyte'
    

)
