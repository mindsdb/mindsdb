from typing import Optional

from mindsdb_sql_parser.ast.base import ASTNode

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.utilities import log
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

import pandas as pd
import pysqream as db

from pysqream_sqlalchemy.dialect import SqreamDialect

logger = log.getLogger(__name__)


class SQreamDBHandler(DatabaseHandler):

    name = 'sqreamdb'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """ Initialize the handler
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)

        self.connection_data = connection_data

        self.connection = None
        self.is_connected = False

    def connect(self):
        """
        Handles the connection to a YugabyteSQL database insance.
        """
        if self.is_connected is True:
            return self.connection

        args = {
            "database": self.connection_data.get('database'),
            "host": self.connection_data.get('host'),
            "port": self.connection_data.get('port'),
            "username": self.connection_data.get('user'),
            "password": self.connection_data.get('password'),
            "clustered": self.connection_data.get('clustered', False),
            "use_ssl": self.connection_data.get('use_ssl', False),
            "service": self.connection_data.get('service', 'sqream')
        }

        connection = db.connect(**args)

        self.is_connected = True
        self.connection = connection
        return self.connection

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the SQreamDB  database
        :return: success status and error message if error occurs
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()
            with connection.cursor() as cur:
                cur.execute('select 1;')
            response.success = True
        except db.Error as e:
            logger.error(f'Error connecting to SQreamDB {self.database}, {e}!')
            response.error_message = e

        if response.success is True and need_to_close:
            self.disconnect()
        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

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

                if cur.rowcount > 0 and query.upper().startswith('SELECT'):
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

    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement
        """
        renderer = SqlalchemyRender(SqreamDialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        List all tables in SQreamDB stored in 'sqream_catalog'
        """

        query = "SELECT table_name FROM sqream_catalog.tables"

        return self.query(query)

    def get_columns(self, table_name):
        query = f"""SELECT column_name, type_name
        FROM sqream_catalog.columns
        WHERE table_name = '{table_name}';
        """
        return self.query(query)
