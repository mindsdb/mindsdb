from operator import truediv
from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.integrations.libs.base_handler import DatabaseHandler
from mindsdb.utilities.log import log
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

import ibm_db



class DB2Handler(DatabaseHandler):


    name= 'DB2'

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.connection_args = kwargs
        self.dbName = kwargs.get('dbName')
        self.userID = kwargs.get('userID')
        self.passWord = kwargs.get('password')
        self.dbConnection = None
        self.is_connected = False
        
        

        
    

    def connect(self) -> HandlerStatusResponse:
        if self.dbConnection is not None:
            return self.check_connection()

        connString = "ATTACH=FALSE"              
        connString += ";DATABASE=" + self.dbName           
        connString += ";PROTOCOL=TCPIP"
        connString += ";UID=" + self.userID
        connString += ";PWD=" + self.passWord

        self.dbConnection = ibm_db.pconnect(connString, '', '')
        self.is_connected= True
        return self.check_connection()


    

    def disconnect(self):
        if self.dbConnection is not None:
            returnCode = ibm_db.close(self.dbConnection)
            if(returnCode):
                self.is_connected=False
                return self.is_connected
            
        return self.is_connected

    


    def check_connection(self) -> HandlerStatusResponse:
        responseCode = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            responseCode.success = True
        except Exception as e:
            log.error(f'Error connecting to SQL Server {self.dbName}, {e}!')
            responseCode.error_message = str(e)
        finally:
            if responseCode.success is True and need_to_close:
                self.disconnect()
            if responseCode.success is False and self.is_connected is True:
                self.is_connected = False

        return responseCode




    def native_query(self, query: Any) -> HandlerStatusResponse:
        pass



    
    def query(self, query: ASTNode) -> HandlerStatusResponse:
        pass

    


    def get_tables(self) -> HandlerStatusResponse:
        pass



    
    def get_columns(self, table_name: str) -> HandlerStatusResponse:
        pass