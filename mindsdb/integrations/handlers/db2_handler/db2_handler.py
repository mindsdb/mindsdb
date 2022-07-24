
from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.integrations.libs.base_handler import DatabaseHandler
from mindsdb.utilities.log import log
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
import pandas as pd
import ibm_db_dbi as love
import ibm_db_sa.ibm_db.DB2Dialect_ibm_db as DB2Dialect




class DB2Handler(DatabaseHandler):


    name= 'DB2'

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        
        self.connection_args = kwargs
        self.driver = "{IBM DB2 ODBC DRIVER}"
        self.dbName = kwargs.get('dbName')
        self.userID = kwargs.get('userID')
        self.passWord = kwargs.get('password')
        self.schemaName = kwargs.get('schemaName')
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        self.connString = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};").format(self.driver, self.dbName, self.host, self.port,"TCPIP" , self.userID, self.passWord)

        self.dbConnection = None 
        self.is_connected = False
        
        

        
    

    def connect(self):
        if self.dbConnection is not None:
            return self.check_connection()

        try:
            self.dbConnection = love.connect(self.connString,'','')
            self.is_connected= True
        except Exception as e:
            log.error(f"Error while connecting to {self.dbName}, {e}")

        
        
        return self.dbConnection


    

    def disconnect(self):
        if self.dbConnection is not None:
            returnCode = self.dbConnection.close()
            if(returnCode):
                self.is_connected=False
                return self.is_connected
            
        return self.is_connected

    


    def check_connection(self) -> StatusResponse:
        responseCode = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            responseCode.success = True
        except Exception as e:
            log.error(f'Error connecting to database {self.dbName}, {e}!')
            responseCode.error_message = str(e)
        finally:
            if responseCode.success is True and need_to_close:
                self.disconnect()
            if responseCode.success is False and self.is_connected is True:
                self.is_connected = False

        return responseCode




    def native_query(self, query: str) -> StatusResponse:

        need_to_close = self.is_connected is False

        self.dbconnection = self.connect()
        # with closing(connection) as con:
        with self.dbconnection.cursor() as cur:
            try:
                cur.execute(query)
                result = cur.fetchall()
                if result:
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        data_frame=pd.DataFrame(
                            result,
                            columns=[x[0] for x in cur.description]
                        )
                    )
                else:
                    response = Response(RESPONSE_TYPE.OK)
                self.dbconnection.commit()
            except Exception as e:
                log.error(f'Error running query: {query} on {self.database}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(e)
                )
                self.dbconnection.rollback()

        if need_to_close is True:
            self.disconnect()

        return response

    
    def query(self, query: ASTNode) -> StatusResponse:
        """
        TODO: Help to finish this
        """


        renderer = SqlalchemyRender(DB2Dialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    


    def get_tables(self) -> StatusResponse:
        q = f"""select tabname as table_name
            from syscat.tables
            where tabschema = '{self.schemaName}' 
            and type = 'T'
            order by tabname"""

        return self.native_query(q)




    
    def get_columns(self, table_name: str) -> StatusResponse:
        

        q = f"""
            SELECT 
            colname as column_name,
            typename as data_type,
            length,
            scale,
            from syscat.columns 
            WHERE 
              tabname = '{table_name.upper()}'
        """

        return self.native_query(q)
