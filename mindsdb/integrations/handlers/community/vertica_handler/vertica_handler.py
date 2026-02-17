from typing import Optional

import pandas as pd
import vertica_python as vp

from mindsdb_sql_parser import parse_sql
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql_parser.ast.base import ASTNode

from mindsdb.utilities import log
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

# from sqlalchemy_vertica.dialect_pyodbc  import VerticaDialect
from sqla_vertica_python.vertica_python import VerticaDialect

logger = log.getLogger(__name__)


class VerticaHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Vertica statements.
    """

    name = 'vertica'

    def __init__(self, name, connection_data: Optional[dict], **kwargs):
        super().__init__(name)

        self.parser = parse_sql
        self.dialect = 'vertica'
        self.kwargs = kwargs
        self.connection_data = connection_data
        self.schema_name = connection_data['schema_name'] if 'schema_name' in connection_data else "public"

        self.connection = None
        self.is_connected = False

    def connect(self):
        if self.is_connected is True:
            return self.connection

        config = {
            'host': self.connection_data['host'],
            'port': self.connection_data['port'],
            'user': self.connection_data['user'],
            'password': self.connection_data['password'],
            'database': self.connection_data['database']
        }

        connection = vp.connect(**config)
        self.is_connected = True
        self.connection = connection
        return self.connection

    def disconnect(self):
        if self.is_connected is False:
            return
        self.connection.close()
        self.is_connected = False
        return

    def check_connection(self) -> StatusResponse:

        result = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()
            result.success = connection.opened()
        except Exception as e:
            logger.error(f'Error connecting to Vertica {self.connection_data["database"]}, {e}!')
            result.error_message = str(e)

        if result.success is True and need_to_close:
            self.disconnect()
        if result.success is False and self.is_connected is True:
            self.is_connected = False

        return result

    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in VERTICA
        :return: returns the records from the current recordset
        """

        need_to_close = self.is_connected is False

        connection = self.connect()
        with connection.cursor() as cur:
            try:
                e = cur.execute(query)
                result = e.fetchall()
                if e.rowcount != -1:

                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        pd.DataFrame(
                            result,
                            columns=[x.name for x in cur.description]
                        )
                    )
                else:
                    response = Response(RESPONSE_TYPE.OK)
                connection.commit()
            except Exception as e:
                logger.error(f'Error running query: {query} on {self.connection_data["database"]}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(e)
                )
                connection.rollback()

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement.
        """
        renderer = SqlalchemyRender(VerticaDialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Get a list with all of the tabels in VERTICA
        """
        q = f'''SELECT
        TABLE_NAME,
        TABLE_SCHEMA
        from v_catalog.tables
        WHERE table_schema='{self.schema_name}'
        order by
        table_name;'''

        return self.native_query(q)

    def get_columns(self, table_name) -> Response:
        """
        Show details about the table
        """
        q = f'''SELECT
        column_name ,
        data_type
        FROM v_catalog.columns
        WHERE table_name='{table_name}';'''

        return self.native_query(q)
