from typing import Optional

import pandas as pd
import ibm_db_dbi
from ibm_db_sa.ibm_db import DB2Dialect_ibm_db as DB2Dialect
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)


logger = log.getLogger(__name__)


class DB2Handler(DatabaseHandler):
    name = "DB2"

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """Initialize the handler
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def connect(self):
        """Set up any connections required by the handler
        Should return output of check_connection() method after attempting
        connection. Should switch self.is_connected.
        Returns:
            Connection Object
        """
        if self.is_connected is True:
            return self.connection
        
        # Mandatory connection parameters.
        if not all(key in self.connection_data for key in ['host', 'user', 'password', 'database']):
            raise ValueError('Required parameters (host, user, password, database) must be provided.')

        connection_string = f"DRIVER={'IBM DB2 ODBC DRIVER'};DATABASE={self.connection_data['database']};HOST={self.connection_data['host']};PROTOCOL=TCPIP;UID={self.connection_data['user']};PWD={self.connection_data['password']};"

        # Optional connection parameters.
        if 'port' in self.connection_data:
            connection_string += f"PORT={self.connection_data['port']};"

        if 'schema' in self.connection_data:
            connection_string += f"CURRENTSCHEMA={self.connection_data['schema']};"

        try:
            self.connection = ibm_db_dbi.pconnect(connection_string, "", "")
            self.is_connected = True
            return self.connection
        except Exception as e:
            logger.error(f"Error while connecting to {self.connection_data.get('database')}, {e}")

    def disconnect(self):
        """Close any existing connections
        Should switch self.is_connected.
        """
        if self.is_connected is False:
            return
        try:
            self.connection.close()
            self.is_connected = False
        except Exception as e:
            logger.error(f"Error while disconnecting to {self.connection_data.get('database')}, {e}")

        return

    def check_connection(self) -> StatusResponse:
        """Check connection to the handler
        Returns:
            HandlerStatusResponse
        """
        responseCode = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            responseCode.success = True
        except Exception as e:
            logger.error(f"Error connecting to database {self.connection_data.get('database')}, {e}!")
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
        query = query.upper()
        conn = self.connect()
        with conn.cursor() as cur:
            try:
                cur.execute(query)

                if cur._result_set_produced:
                    result = cur.fetchall()
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        data_frame=pd.DataFrame(
                            result, columns=[x[0] for x in cur.description]
                        ),
                    )
                else:
                    response = Response(RESPONSE_TYPE.OK)
                self.connection.commit()
            except Exception as e:
                logger.error(f"Error running query: {query} on {self.connection_data.get('database')}!")
                response = Response(RESPONSE_TYPE.ERROR, error_message=str(e))
                self.connection.rollback()

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> StatusResponse:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INTSERT, DELETE, etc
        Returns:
            HandlerResponse
        """

        renderer = SqlalchemyRender(DB2Dialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> StatusResponse:
        """Return list of entities
        Return list of entities that will be accesible as tables.
        Returns:
            HandlerResponse: shoud have same columns as information_schema.tables
                (https://dev.mysql.com/doc/refman/8.0/en/information-schema-tables-table.html)
                Column 'TABLE_NAME' is mandatory, other is optional.
        """
        self.connect()

        result = self.connection.tables(self.connection.current_schema)

        tables = []
        for table in result:
            tables.append(
                {
                    "TABLE_NAME": table["TABLE_NAME"],
                    "TABLE_SCHEMA": table["TABLE_SCHEM"],
                    "TABLE_TYPE": table["TABLE_TYPE"],
                }
            )

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(tables)
        )

        return response

    def get_columns(self, table_name: str) -> StatusResponse:
        """Returns a list of entity columns
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            HandlerResponse: shoud have same columns as information_schema.columns
                (https://dev.mysql.com/doc/refman/8.0/en/information-schema-columns-table.html)
                Column 'COLUMN_NAME' is mandatory, other is optional. Hightly
                recomended to define also 'DATA_TYPE': it should be one of
                python data types (by default it str).
        """

        self.connect()

        result = self.connection.columns(table_name=table_name)
        try:
            if result:
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=pd.DataFrame(
                        [result[i]["COLUMN_NAME"] for i in range(len(result))],
                        columns=["COLUMN_NAME"],
                    ),
                )
            else:
                response = Response(RESPONSE_TYPE.OK)

        except Exception as e:
            logger.error(f"Error running while getting table {e} on ")
            response = Response(RESPONSE_TYPE.ERROR, error_message=str(e))

        return response
