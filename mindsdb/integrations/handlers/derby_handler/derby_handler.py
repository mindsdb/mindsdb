from collections import OrderedDict
from typing import Optional
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.utilities import log
from mindsdb_sql import parse_sql
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
import pandas as pd
import jaydebeapi as jdbcconnector

logger = log.getLogger(__name__)

class DerbyHandler(DatabaseHandler):


    name= 'derby'


    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """ Initialize the handler
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        
        self.kwargs = kwargs
        self.parser = parse_sql
        self.database = connection_data['database']
        self.connection_config = connection_data
        self.host = connection_data['host']
        self.port = connection_data['port']
        self.schema =  'APP'
        self.connection = None
        self.is_connected = False
          
    
    def connect(self):
        """ Set up any connections required by the handler
        Should return output of check_connection() method after attempting
        connection. Should switch self.is_connected.
        Returns:
            Connection Object
        """
        if self.is_connected is True:
            return self.connection

        user = self.connection_config.get('user')
        password = self.connection_config.get('password')
        jdbc_class = self.connection_config.get('jdbcClass')
        jar_location = self.connection_config.get('jdbcJarLocation')

        jdbc_url = "jdbc:derby://" + self.host + ":" + self.port + "/" + self.database + ";"

        if not jdbc_class: 
            jdbc_class = "org.apache.derby.jdbc.ClientDriver"

        if user: 
            self.schema = user

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
            logger.error(f"Error while connecting to {self.database}, {e}")

        return self.connection


    def disconnect(self):
        """ Close any existing connections
        Should switch self.is_connected.
        """
        if self.is_connected is False:
            return
        try:
            self.connection.close()
            self.is_connected=False
        except Exception as e:
            logger.error(f"Error while disconnecting to {self.database}, {e}")

        return 


    def check_connection(self) -> StatusResponse:
        """ Check connection to the handler
        Returns:
            HandlerStatusResponse
        """
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
                self.connection.commit()
            except Exception as e:
                logger.error(f'Error running query: {query} on {self.database}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(e)
                )
                self.connection.rollback()

        if need_to_close is True:
            self.disconnect()

        return response

    
    def query(self, query: ASTNode) -> StatusResponse:
        """Render and execute a SQL query.

        Args:
            query (ASTNode): The SQL query.

        Returns:
            Response: The query result.
        """
        if isinstance(query, ASTNode):
            query_str = query.to_string()
        else:
            query_str = str(query)

        return self.native_query(query_str)


    def get_tables(self) -> StatusResponse:
        """Get a list of all the tables in the database.

        Returns:
            Response: Names of the tables in the database.
        """
        query = f'''
        SELECT st.tablename FROM sys.systables st LEFT OUTER JOIN sys.sysschemas ss ON (st.schemaid = ss.schemaid) WHERE ss.schemaname ='{self.schema}' '''
    
        result = self.native_query(query)
        df = result.data_frame
        result.data_frame = df.rename(columns={df.columns[0]: 'table_name'})
        return result

    
    def get_columns(self, table_name: str) -> StatusResponse:
        """Get details about a table.

        Args:
            table_name (str): Name of the table to retrieve details of.

        Returns:
            Response: Details of the table.
        """

        query = f''' SELECT COLUMNNAME FROM SYS.SYSCOLUMNS INNER JOIN SYS.SYSTABLES ON SYS.SYSCOLUMNS.REFERENCEID=SYS.SYSTABLES.TABLEID WHERE TABLENAME='{table_name}' '''
        return self.native_query(query)
    
    
    connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the Apache Derby server/database.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'Specify port to connect to Apache Derby.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': """
            The database name to use when connecting with the Apache Derby server.
        """
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Apache Derby server. If specified this is also treated as the schema.'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'The password to authenticate the user with the Apache Derby server.'
    },
    jdbcClass={
        'type': ARG_TYPE.STR,
        'description': 'The jdbc class which should be used to establish the connection, the default value is:  org.apache.derby.jdbc.ClientDriver.'
    },
    jdbcJarLocation={
        'type': ARG_TYPE.STR,
        'description': 'The location of the jar files which contain the JDBC class. This need not be specified if the required classes are already added to the CLASSPATH variable.'
    }

)


connection_args_example = OrderedDict(
    host='localhost',
    port='1527',
    user='test',
    password='test',
    database="testdb",
    jdbcClass='org.apache.derby.jdbc.ClientDriver',
    jdbcJarLocation='/opt/homebrew/Cellar/derby/10.16.1.1/libexec/lib/derby.jar,/opt/homebrew/Cellar/derby/10.16.1.1/libexec/lib/derbyclient.jar,/opt/homebrew/Cellar/derby/10.16.1.1/libexec/lib/derbynet.jar,/opt/homebrew/Cellar/derby/10.16.1.1/libexec/lib/derbyoptionaltools.jar,/opt/homebrew/Cellar/derby/10.16.1.1/libexec/lib/derbyrun.jar,/opt/homebrew/Cellar/derby/10.16.1.1/libexec/lib/derbyshared.jar,/opt/homebrew/Cellar/derby/10.16.1.1/libexec/lib/derbytools.jar',
)

