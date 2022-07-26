from collections import OrderedDict

from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.integrations.libs.base_handler import DatabaseHandler
from mindsdb.utilities.log import log
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE



import pandas as pd
import ibm_db_dbi as love

from ibm_db_sa.ibm_db import DB2Dialect_ibm_db as DB2Dialect




class DB2Handler(DatabaseHandler):


    name= 'DB2'

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        
        self.connection_args = kwargs
        self.driver = "{IBM DB2 ODBC DRIVER}"
        self.database = kwargs.get('database')
        self.user = kwargs.get('user')
        self.password = kwargs.get('password')
        self.schemaName = kwargs.get('schemaName')
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        self.connString = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOST={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};").format(self.driver, self.database, self.host, self.port,"TCPIP" , self.user, self.password)
        

        self.connection = None
        self.is_connected = False
        
        

        
    

    def connect(self):
        # if self.is_connected is True:
        #     return self.connection

        try:
            self.connection = love.pconnect(self.connString,'','')
  
            self.is_connected= True
        except Exception as e:
            log.error(f"Error while connecting to {self.database}, {e}")


        return self.connection


    

    def disconnect(self):
        if self.is_connected is False:
            return
        try:
            self.connection.close()
        except Exception as e:
            log.error(f"Error while disconnecting to {self.database}, {e}")

        return 

    


    def check_connection(self) -> StatusResponse:
        responseCode = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            responseCode.success = True
        except Exception as e:
            log.error(f'Error connecting to database {self.database}, {e}!')
            responseCode.error_message = str(e)
        finally:
            if responseCode.success is True and need_to_close:
                self.disconnect()
            if responseCode.success is False and self.is_connected is True:
                self.is_connected = False

        return responseCode




    def native_query(self, query: str) -> StatusResponse:
        need_to_close = self.is_connected is False
        
        conn = self.connect()
        with conn.cursor() as cur:
            try:
                cur.execute(query)
                   
                if cur._result_set_produced :
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
                self.connection.commit()
            except Exception as e:
                log.error(f'Error running query: {query} on {self.database}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(e)
                )
                self.connection.rollback()

        if need_to_close is True:
            self.disconnect()

        return response

    
    def query(self, query: ASTNode) -> StatusResponse:
        """
        TODO: Check this method 
        """


        renderer = SqlalchemyRender(DB2Dialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    


    def get_tables(self) -> StatusResponse:
        self.connect()


        result=self.connection.tables(self.schemaName)
        try:
            if result:
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=pd.DataFrame(
                        [result[i]['TABLE_NAME'] for i in range(len(result)) ],
                        columns=['TABLE_NAME']
                        
                    )
                )
            else:
                response = Response(RESPONSE_TYPE.OK)
            
        except Exception as e:
            log.error(f'Error running while getting table {e} on ')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )



        return response




    
    def get_columns(self, table_name: str) -> StatusResponse:
        
        self.connect()


        result=self.connection.columns(table_name)
        try:
            if result:
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=pd.DataFrame(
                        [result[i]['COLUMN_NAME'] for i in range(len(result)) ],
                        columns=['COLUMN_NAME']
                        
                    )
                )
            else:
                response = Response(RESPONSE_TYPE.OK)
            
        except Exception as e:
            log.error(f'Error running while getting table {e} on ')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )



        return response

        





connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the DB2 server/database.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': """
            The database name to use when connecting with the DB2 server.
        """
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the DB2 server.'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'The password to authenticate the user with the DB2 server.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'Specify port to connect DB2 through TCP/IP'
    },
    schemaName={
        'type': ARG_TYPE.STR,
        'description': 'Specify the schema name '
    },

)

connection_args_example = OrderedDict(
    host='127.0.0.1',
    port='25000',
    password='1234',
    user='db2admin',
    schemaName="db2admin",
    database="BOOKS",
)