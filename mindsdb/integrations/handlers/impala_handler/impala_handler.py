from collections import OrderedDict
from typing import Optional
import pandas as pd
from impala import dbapi as db , sqlalchemy as SA

from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.utilities.log import get_log
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


log = get_log()


class ImpalaHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Impala statements.
    """

    name = 'impala'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        super().__init__(name)

        
        self.parser = parse_sql
        self.dialect = 'impala'
        self.kwargs = kwargs
        self.connection_data = connection_data

        self.connection = None
        self.is_connected = False



    def connect(self):
        if self.is_connected is True:
            return self.connection

        config = {
            'host': self.connection_data.get('host'),
            'port': self.connection_data.get('port',21050),
            'user': self.connection_data.get('user'),
            'password': self.connection_data.get('password'),
            'database': self.connection_data.get('database'),
            
            
        }

        connection = db.connect(**config)
        self.is_connected = True
        self.connection = connection
        return self.connection

    def disconnect(self):
        if self.is_connected is False:
            return
        self.connection.close()
        self.is_connected = False
        return

    def check_connection(self) -> StatusResponse:
       
        result = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()
            result.success = connection is not None
        except Exception as e:
            log.error(f'x x x Error connecting to Impala {self.connection_data["database"]}, {e}!')
            result.error_message = str(e)

        if result.success is True and need_to_close:
            self.disconnect()
        if result.success is False and self.is_connected is True:
            self.is_connected = False

        return result

    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in Impala
        :return: returns the records from the current recordset
        """

        need_to_close = self.is_connected is False

        connection = self.connect()
        with connection.cursor() as cur:
            try:
                cur.execute(query)
                result = cur.fetchall()
                if cur.has_result_set:
                    
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        pd.DataFrame(
                            result,
                            columns=[x[0] for x in cur.description]
                        )
                    )
                else:
                    response = Response(RESPONSE_TYPE.OK)
                connection.commit()
            except Exception as e:
                log.error(f'Error running query: {query} on {self.connection_data["database"]}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(e)
                )
                # connection.rollback()

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement.
        """
        renderer = SqlalchemyRender(SA.ImpalaDialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Get a list with all of the tabels in Impala
        """
        q = "SHOW TABLES;"
        result= self.native_query(q)
        df = result.data_frame.rename(columns={'name': 'TABLE_NAME'})
        result.data_frame = df

        return result

    def get_columns(self, table_name: str) -> Response:
        """
        Show details about the table
        """
        q = f"DESCRIBE {table_name};"

        result= self.native_query(q)
        df = result.data_frame.iloc[:,0:2].rename(columns={'name': 'COLUMN_NAME', 'type': 'Data_Type'})
        result.data_frame = df

        return result




connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Impala server.'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'The password to authenticate the user with the Impala server.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the Impala server.'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the Impala server.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the Impala server. Must be an integer. Default is 21050'
    }

)

connection_args_example = OrderedDict(

    host='127.0.0.1',
    port=21050,
    user='USERNAME',
    password='P@55W0Rd',
    database='D4t4bA5e'
    
    )
