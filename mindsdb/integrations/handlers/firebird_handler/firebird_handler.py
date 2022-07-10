from typing import Optional
from collections import OrderedDict

import pandas as pd
import fdb

from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.libs.base_handler import DatabaseHandler

from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.utilities.log import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


class FirebirdHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Firebird statements.
    """

    name = 'firebird'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = 'firebird'
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> StatusResponse:
        """
        Set up the connection required by the handler.
        Returns:
            HandlerStatusResponse
        """

        if self.is_connected is True:
            return self.connection

        self.connection = fdb.connect(
            host=self.connection_data['host'],
            database=self.connection_data['database'],
            user=self.connection_data['user'],
            password=self.connection_data['password'],
        )
        self.is_connected = True

        return self.connection

    def disconnect(self):
        """
        Close any existing connections.
        """

        if self.is_connected is False:
            return

        self.connection.close()
        self.is_connected = False
        return self.is_connected

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """

        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            response.success = True
        except Exception as e:
            log.error(f'Error connecting to Firebird {self.connection_data["database"]}, {e}!')
            response.error_message = str(e)
        finally:
            if response.success is True and need_to_close:
                self.disconnect()
            if response.success is False and self.is_connected is True:
                self.is_connected = False

        return response

    def native_query(self, query: str) -> StatusResponse:
        """
        Receive raw query and act upon it somehow.
        Args:
            query (str): query in native format
        Returns:
            HandlerResponse
        """

        need_to_close = self.is_connected is False

        connection = self.connect()
        cursor = connection.cursor()

        try:
            cursor.execute(query)
            result = cursor.fetchall()
            if result:
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=pd.DataFrame(
                        result,
                        columns=[x[0] for x in cursor.description]
                    )
                )
            else:
                connection.commit()
                response = Response(RESPONSE_TYPE.OK)
        except Exception as e:
            log.error(f'Error running query: {query} on {self.connection_data["database"]}!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

        cursor.close()
        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> StatusResponse:
        """
        Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INTSERT, DELETE, etc
        Returns:
            HandlerResponse
        """
        renderer = SqlalchemyRender('firebird')
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> StatusResponse:
        """
        Return list of entities that will be accessible as tables.
        Returns:
            HandlerResponse
        """

        query = """
            SELECT RDB$RELATION_NAME 
            FROM RDB$RELATIONS
            WHERE (RDB$SYSTEM_FLAG <> 1 OR RDB$SYSTEM_FLAG IS NULL) AND RDB$VIEW_BLR IS NULL
            ORDER BY RDB$RELATION_NAME;
        """
        result = self.native_query(query)
        df = result.data_frame
        df[df.columns[0]] = df[df.columns[0]].apply(lambda row: row.strip())
        result.data_frame = df.rename(columns={df.columns[0]: 'table_name'})
        return result

    def get_columns(self, table_name: str) -> StatusResponse:
        """
        Returns a list of entity columns.
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            HandlerResponse
        """

        query = f"""
            SELECT
              RF.RDB$FIELD_NAME FIELD_NAME,
              CASE F.RDB$FIELD_TYPE
                WHEN 7 THEN
                  CASE F.RDB$FIELD_SUB_TYPE
                    WHEN 0 THEN 'SMALLINT'
                    WHEN 1 THEN 'NUMERIC(' || F.RDB$FIELD_PRECISION || ', ' || (-F.RDB$FIELD_SCALE) || ')'
                    WHEN 2 THEN 'DECIMAL'
                  END
                WHEN 8 THEN
                  CASE F.RDB$FIELD_SUB_TYPE
                    WHEN 0 THEN 'INTEGER'
                    WHEN 1 THEN 'NUMERIC('  || F.RDB$FIELD_PRECISION || ', ' || (-F.RDB$FIELD_SCALE) || ')'
                    WHEN 2 THEN 'DECIMAL'
                  END
                WHEN 9 THEN 'QUAD'
                WHEN 10 THEN 'FLOAT'
                WHEN 12 THEN 'DATE'
                WHEN 13 THEN 'TIME'
                WHEN 14 THEN 'CHAR(' || (TRUNC(F.RDB$FIELD_LENGTH / CH.RDB$BYTES_PER_CHARACTER)) || ') '
                WHEN 16 THEN
                  CASE F.RDB$FIELD_SUB_TYPE
                    WHEN 0 THEN 'BIGINT'
                    WHEN 1 THEN 'NUMERIC(' || F.RDB$FIELD_PRECISION || ', ' || (-F.RDB$FIELD_SCALE) || ')'
                    WHEN 2 THEN 'DECIMAL'
                  END
                WHEN 27 THEN 'DOUBLE'
                WHEN 35 THEN 'TIMESTAMP'
                WHEN 37 THEN 'VARCHAR(' || (TRUNC(F.RDB$FIELD_LENGTH / CH.RDB$BYTES_PER_CHARACTER)) || ')'
                WHEN 40 THEN 'CSTRING' || (TRUNC(F.RDB$FIELD_LENGTH / CH.RDB$BYTES_PER_CHARACTER)) || ')'
                WHEN 45 THEN 'BLOB_ID'
                WHEN 261 THEN 'BLOB SUB_TYPE ' || F.RDB$FIELD_SUB_TYPE
                ELSE 'RDB$FIELD_TYPE: ' || F.RDB$FIELD_TYPE || '?'
              END FIELD_TYPE,
              IIF(COALESCE(RF.RDB$NULL_FLAG, 0) = 0, NULL, 'NOT NULL') FIELD_NULL,
              CH.RDB$CHARACTER_SET_NAME FIELD_CHARSET,
              DCO.RDB$COLLATION_NAME FIELD_COLLATION,
              COALESCE(RF.RDB$DEFAULT_SOURCE, F.RDB$DEFAULT_SOURCE) FIELD_DEFAULT,
              F.RDB$VALIDATION_SOURCE FIELD_CHECK,
              RF.RDB$DESCRIPTION FIELD_DESCRIPTION
            FROM RDB$RELATION_FIELDS RF
            JOIN RDB$FIELDS F ON (F.RDB$FIELD_NAME = RF.RDB$FIELD_SOURCE)
            LEFT OUTER JOIN RDB$CHARACTER_SETS CH ON (CH.RDB$CHARACTER_SET_ID = F.RDB$CHARACTER_SET_ID)
            LEFT OUTER JOIN RDB$COLLATIONS DCO ON ((DCO.RDB$COLLATION_ID = F.RDB$COLLATION_ID) AND (DCO.RDB$CHARACTER_SET_ID = F.RDB$CHARACTER_SET_ID))
            WHERE (RF.RDB$RELATION_NAME = '{table_name.upper()}') AND (COALESCE(RF.RDB$SYSTEM_FLAG, 0) = 0)
            ORDER BY RF.RDB$FIELD_POSITION;
        """
        result = self.native_query(query)
        df = result.data_frame
        result.data_frame = df.rename(columns={'FIELD_NAME': 'column_name', 'FIELD_TYPE': 'data_type'})
        return result


connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the Firebird server.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': """
            The database name to use when connecting with the Firebird server. NOTE: use double backslashes (\\) for the 
            database path on a Windows machine.
        """
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Firebird server.'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'The password to authenticate the user with the Firebird server.'
    }
)

connection_args_example = OrderedDict(
    host='localhost',
    database='/temp/test.db',
    user='sysdba',
    password='password'
)