from collections import OrderedDict
from typing import Optional
from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.utilities import log
from mindsdb_sql_parser import parse_sql
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE



import pandas as pd
import IfxPyDbi as I 
from sqlalchemy_informix.ibmdb import  InformixDialect

logger = log.getLogger(__name__)

class InformixHandler(DatabaseHandler):


    name= 'informix'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        ''' Initialize the handler
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        '''
        super().__init__(name)
        
        self.kwargs = kwargs
        self.parser = parse_sql
        self.loging_enabled = connection_data['loging_enabled'] if 'loging_enabled' in connection_data else True
        self.server = connection_data['server']
        self.database = connection_data['database']
        self.user = connection_data['user']
        self.password = connection_data['password']
        self.schemaName = connection_data['schema_name'] 
        self.host = connection_data['host']
        self.port = connection_data['port']
        self.connString = (
    'SERVER={0};'
    'DATABASE={1};'
    'HOST={2};'
    'PORT={3};'
    'UID={4};'
    'PWD={5};').format(self.server, self.database, self.host, self.port, self.user, self.password)

        
        

        self.connection = None
        self.is_connected = False
          
    
    def connect(self):
        ''' Set up any connections required by the handler
        Should return output of check_connection() method after attempting
        connection. Should switch self.is_connected.
        Returns:
            Connection Object
        '''
        if self.is_connected is True:
            return self.connection

        try:
            self.connection = I.connect(self.connString,'','')
  
            self.is_connected= True
        except Exception as e:
            logger.error(f'Error while connecting to {self.database}, {e}')


        return self.connection


    def disconnect(self):
        ''' Close any existing connections
        Should switch self.is_connected.
        '''
        if self.is_connected is False:
            return
        try:
            self.connection.close()
            self.is_connected = False
        except Exception as e:
            logger.error(f'Error while disconnecting to {self.database}, {e}')

        return 


    def check_connection(self) -> StatusResponse:
        ''' Check connection to the handler
        Returns:
            HandlerStatusResponse
        '''
        responseCode = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            responseCode.success = True
        except Exception as e:
            logger.error(f'Error connecting to database {self.database}, {e}!')
            responseCode.error_message = str(e)
        finally:
            if responseCode.success is True and need_to_close:
                self.disconnect()
            if responseCode.success is False and self.is_connected is True:
                self.is_connected = False

        return responseCode


    def native_query(self, query: str) -> StatusResponse:
        '''Receive raw query and act upon it somehow.
        Args:
            query (Any): query in native format (str for sql databases,
                dict for mongo, etc)
        Returns:
            HandlerResponse
        '''
        need_to_close = self.is_connected is False
        conn = self.connect()
        cur = conn.cursor()
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
                if self.loging_enabled:
                    self.connection.commit()
        except Exception as e:
            logger.error(f'Error running query: {query} on {self.database}')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )
            if self.loging_enabled:
                self.connection.rollback()

            cur.close()


        if need_to_close is True:
            self.disconnect()

        return response

    
    def query(self, query: ASTNode) -> StatusResponse:
        '''Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INTSERT, DELETE, etc
        Returns:
            HandlerResponse
        '''
        


        renderer = SqlalchemyRender(InformixDialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)


    def get_tables(self) -> StatusResponse:
        ''' Return list of entities
        Return list of entities that will be accesible as tables.
        Returns:
            HandlerResponse: shoud have same columns as information_schema.tables
                (https://dev.mysql.com/doc/refman/8.0/en/information-schema-tables-table.html)
                Column 'TABLE_NAME' is mandatory, other is optional.
        '''
        self.connect()


        result=self.connection.tables()
        try:
            if result:
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=pd.DataFrame(
                        [x['TABLE_NAME'] for x in result if x['TABLE_SCHEM']==self.schemaName],
                        columns=['TABLE_NAME']
                        
                    )
                )
            else:
                response = Response(RESPONSE_TYPE.OK)
            
        except Exception as e:
            logger.error(f'Error running while getting table {e} on ')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )



        return response

    
    def get_columns(self, table_name: str) -> StatusResponse:
        ''' Returns a list of entity columns
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            HandlerResponse: shoud have same columns as information_schema.columns
                (https://dev.mysql.com/doc/refman/8.0/en/information-schema-columns-table.html)
                Column 'COLUMN_NAME' is mandatory, other is optional. Hightly
                recomended to define also 'DATA_TYPE': it should be one of
                python data types (by default it str).
        '''
        
        self.connect()


        result=self.connection.columns(table_name=table_name)
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
            logger.error(f'Error running while getting table {e} on ')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )



        return response

        





connection_args = OrderedDict(
    server={
        'type': ARG_TYPE.STR,
        'description': '''
            The server name you want to get connected.
        '''
    },
    database={
        'type': ARG_TYPE.STR,
        'description': '''
            The database name to use when connecting with the DB2 server.
        '''
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the DB2 server.'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'The password to authenticate the user with the DB2 server.'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the DB2 server/database.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'Specify port to connect DB2 through TCP/IP'
    },
    schema_name={
        'type': ARG_TYPE.STR,
        'description': 'Specify the schema name for showing tables '
    },
    logging_enabled={
        'type': ARG_TYPE.BOOL,
        'description': '''
        Used for COMMIT and ROLLBACK as this command works only for logging enabled database.
        Note: Its optional.
        Default is TRUE
        '''
    }

)

connection_args_example = OrderedDict(
    server='server',
    database='stores_demo',
    user='informix',
    password='in4mix',
    host='127.0.0.1',
    port='9091',
    schema_name='Love'
    
)
