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

import pandas as pd
import pymonetdb as mdb
from .utils.monet_get_id import schema_id, table_id
from sqlalchemy_monetdb.dialect import MonetDialect

logger = log.getLogger(__name__)


class MonetDBHandler(DatabaseHandler):
    name = "monetdb"

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
        self.database = connection_data["database"]
        self.user = connection_data["user"]
        self.password = connection_data["password"]
        self.schemaName = (
            connection_data["schema_name"] if "schema_name" in connection_data else None
        )
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
        if self.is_connected is True:
            return self.connection

        try:
            self.connection = mdb.connect(
                database=self.database,
                hostname=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
            )

            self.is_connected = True
        except Exception as e:
            logger.error(f"Error while connecting to {self.database}, {e}")

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
            logger.error(f"Error while disconnecting to {self.database}, {e}")

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
            logger.error(f"Error connecting to database {self.database}, {e}!")
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

            if len(cur._rows) > 0:
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
            logger.error(f"Error running query: {query} on {self.database}!")
            response = Response(RESPONSE_TYPE.ERROR, error_message=str(e))
            self.connection.rollback()

        cur.close()

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> StatusResponse:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INTSERT, DELETE, etc
        Returns: HandlerResponse
        """

        renderer = SqlalchemyRender(MonetDialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> StatusResponse:
        """Return list of entities
        Return list of entities that will be accesible as tables.
        Returns: HandlerResponse: shoud have same columns as information_schema.tables
                (https://dev.mysql.com/doc/refman/8.0/en/information-schema-tables-table.html)
                Column 'TABLE_NAME' is mandatory, other is optional.
        """
        self.connect()
        schema = schema_id(connection=self.connection, schema_name=self.schemaName)

        q = f"""
            SELECT name as TABLE_NAME
            FROM sys.tables
            WHERE system = False
            AND type = 0
            AND schema_id = {schema}
        """

        return self.query(q)

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
        table = table_id(
            connection=self.connection,
            table_name=table_name,
            schema_name=self.schemaName,
        )

        q = f"""
            SELECT
            name as COLUMN_NAME,
            type as DATA_TYPE
            FROM sys.columns
            WHERE table_id = {table}
        """
        return self.query(q)
