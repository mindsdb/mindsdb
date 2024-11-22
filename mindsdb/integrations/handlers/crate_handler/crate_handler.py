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
    RESPONSE_TYPE,
)
from mindsdb.integrations.libs.const import (
    HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE,
)


import pandas as pd
from crate import client as db
from sqlalchemy_cratedb import dialect

logger = log.getLogger(__name__)


class CrateHandler(DatabaseHandler):
    name = "crate"

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """Initialize the handler
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)

        self.kwargs = kwargs
        self.parser = parse_sql
        self.dialect = "crate"
        self.user = connection_data["user"]
        self.password = connection_data["password"]
        self.schemaName = connection_data.get("schema_name", "doc")
        self.host = connection_data["host"]
        self.port = connection_data["port"]

        self.connection = None
        self.is_connected = False

    def connect(self):
        """Set up any connections required by the handler
        Should return output of check_connection() method after attempting
        connection. Should switch self.is_connected.
        Returns:
            Connection Object
        """
        if self.is_connected:
            return self.connection

        is_local = (
            self.host.startswith("localhost") or self.host == "127.0.0.1"
        )

        try:
            # Build URL based on connection type
            protocol = "http" if is_local else "https"
            url = f"{protocol}://{self.user}:{self.password}@{self.host}:{self.port}"

            # Connect with appropriate settings based on connection type
            self.connection = db.connect(
                url,
                timeout=30,
                # Only verify SSL for cloud connections
                verify_ssl_cert=not is_local,
            )

            self.is_connected = True
        except Exception as e:
            logger.error(f"Error while connecting to CrateDB: {e}")

        return self.connection

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
            logger.error(f"Error while disconnecting to CrateDB, {e}")

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
            logger.error(f"Error connecting to  CrateDB, {e}!")
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
        cur = conn.cursor()
        try:
            cur.execute(query)
            if cur.rowcount:
                result = cur.fetchall()
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=pd.DataFrame(
                        result, columns=[x[0] for x in cur.description]
                    ),
                )
            else:
                response = Response(RESPONSE_TYPE.OK)
        except Exception as e:
            logger.error(f"Error running query: {query} on CrateDB!")
            response = Response(RESPONSE_TYPE.ERROR, error_message=str(e))
        cur.close()

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

        renderer = SqlalchemyRender(dialect)
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

        q = f"SHOW TABLES FROM {self.schemaName};"
        result = self.native_query(q)
        return result

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

        q = f"SHOW COLUMNS FROM {table_name};"
        result = self.native_query(q)
        return result


connection_args = OrderedDict(
    host={
        "type": ARG_TYPE.STR,
        "description": "The host name or IP address of the CrateDB server/database.",
    },
    user={
        "type": ARG_TYPE.STR,
        "description": "The user name used to authenticate with the CrateDB server.",
    },
    password={
        "type": ARG_TYPE.STR,
        "description": "The password to authenticate the user with the CrateDB server.",
    },
    port={
        "type": ARG_TYPE.INT,
        "description": "Specify port to connect CrateDB server",
    },
    schemaName={
        "type": ARG_TYPE.STR,
        "description": 'Specify the schema name. Note: It is optional DEFAULT is "doc"',
    },
)

connection_args_example = OrderedDict(
    host="127.0.0.1",
    port="4200",
    password="",
    user="crate",
)
