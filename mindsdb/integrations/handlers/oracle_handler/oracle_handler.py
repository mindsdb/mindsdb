from typing import Optional

import pandas as pd
import oracledb
from oracledb import connect, Connection, makedsn

from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.utilities import log
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)

oracledb.defaults.fetch_lobs = False  # return LOBs directly as strings or bytes

logger = log.getLogger(__name__)

class OracleHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Microsoft SQL Server statements.
    """

    name = "oracle"

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        super().__init__(name)
        self.host = connection_data.get("host")
        self.port = int(connection_data.get("port") or 1521)
        self.sid = connection_data.get("sid")
        self.service_name = connection_data.get("service_name")
        self.user = connection_data.get("user")
        self.password = connection_data.get("password")
        self.disable_oob = bool(connection_data.get("disable_oob"))

        self.auth_mode = None
        if 'auth_mode' in connection_data:
            mode_name = 'AUTH_MODE_' + connection_data['auth_mode'].upper()
            if not hasattr(oracledb, mode_name):
                raise ValueError(f'Unknown auth mode: {mode_name}')
            self.auth_mode = getattr(oracledb, mode_name)

        if self.sid is None and self.service_name is None:
            raise ValueError("Either 'sid' or 'service_name' must be given")
        if self.sid and self.service_name:
            raise ValueError("Only one of 'sid' or 'service_name' must be given")

        if self.sid:
            self.dsn = makedsn(host=self.host, port=self.port, sid=self.sid)
        else:
            self.dsn = makedsn(host=self.host, port=self.port, service_name=self.service_name)

        self.connection = None
        self.is_connected = False

    def connect(self) -> Connection:
        if self.is_connected is True:
            return self.connection
        d = None  # default suitable for Linux OS
        oracledb.init_oracle_client(lib_dir=d)
        connection = connect(
            user=self.user, password=self.password, dsn=self.dsn,
            disable_oob=self.disable_oob, mode=self.auth_mode,
        )

        self.is_connected = True
        self.connection = connection
        return self.connection

    def disconnect(self):
        if self.is_connected:
            self.connection.close()
        self.is_connected = False
        return

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the database
        :return: success status and error message if error occurs
        """

        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            con = self.connect()
            con.ping()
            response.success = True
        except Exception as e:
            logger.error(f"Error connecting to Oracle DB {self.dsn}, {e}!")
            response.error_message = str(e)
        finally:
            if response.success is True and need_to_close:
                self.disconnect()
            if response.success is False and self.is_connected is True:
                self.is_connected = False
        return response

    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run
        :return: returns the records from the current recordset
        """
        need_to_close = self.is_connected is False

        connection = self.connect()
        with connection.cursor() as cur:
            try:
                cur.execute(query)
                result = cur.fetchall()
                if result:
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        data_frame=pd.DataFrame(
                            result,
                            columns=[row[0] for row in cur.description],
                        ),
                    )
                else:
                    response = Response(RESPONSE_TYPE.OK)

                connection.commit()
            except Exception as e:
                logger.error(f"Error running query: {query} on {self.dsn}!")
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(e),
                )
                connection.rollback()

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement.
        """
        renderer = SqlalchemyRender("oracle")
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        List all tables in Oracle DB owned by the current user.
        """
        query = """
            SELECT table_name
            FROM user_tables
            ORDER BY 1
        """
        return self.native_query(query)

    def get_columns(self, table_name: str) -> Response:
        """
        Show details about the table.
        """
        query = f"""
            SELECT 
                column_name,
                data_type
            FROM USER_TAB_COLUMNS
            WHERE table_name = '{table_name}'
        """
        result = self.native_query(query)
        return result
