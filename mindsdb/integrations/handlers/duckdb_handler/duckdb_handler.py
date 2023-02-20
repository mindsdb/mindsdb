import duckdb
from collections import OrderedDict

from mindsdb_sql import parse_sql
from mindsdb.integrations.libs.base import DatabaseHandler

from mindsdb_sql.parser.ast.base import ASTNode


from mindsdb.utilities import log

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


class DuckDBHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the DuckDB statements.
    """

    name = "duckdb"

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = "duckdb"
        self.connection_data = kwargs.get("connection_data")

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self):
        """
        Handles the connection to a DuckDB database instance.
        """

        if self.is_connected is True:
            return self.connection

        self.connection = duckdb.connect(self.connection_data['db_file'])
        self.is_connected = True

        return self.connection

    def disconnect(self):
        pass

    def check_connection(self) -> StatusResponse:
        
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            response.success = True
        except Exception as e:
            log.logger.error(f'Error connecting to DuckDB {self.connection_data["db_file"]}, {e}!')
            response.error_message = str(e)
        finally:
            if response.success is True and need_to_close:
                self.disconnect()
            if response.success is False and self.is_connected is True:
                self.is_connected = False

        return response

    def native_query(self, query: str) -> Response:
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
                        result, columns=[x[0] for x in cursor.description]
                    ),
                )
            else:
                connection.commit()
                response = Response(RESPONSE_TYPE.OK)
        except Exception as e:
            log.logger.error(
                f'Error running query: {query} on {self.connection_data["db_file"]}!'
            )
            response = Response(RESPONSE_TYPE.ERROR, error_message=str(e))

        cursor.close()
        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        query_str = query.to_string()
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Get a list of all of the table in the database
        Returns:
            HandlerResponse
        """

        q = "SHOW TABLES;"
        result = self.native_query(q)
        df = result.data_frame
        result.data_frame = df.rename(columns={df.columns[0]: "table_name"})
        return result

    def get_columns(self, table_name: str) -> Response:
        """
        Return details about the table columns.
        Args:
            table_name (str): name of one of table
        Returns:
            HandlerResponse
        """

        query = f"DESCRIBE {table_name};"
        return self.native_query(query)


connection_args = OrderedDict(
    database={
        "type": ARG_TYPE.STR,
        "description": "The database file to read and write from. The special value :memory: (default) can be used to create an in-memory database.",
    },
    read_only={
        "type": ARG_TYPE.BOOL,
        "description": "A flag that specifies if the connection should be made in read-only mode.",
    },
)

connection_args_example = OrderedDict(database="database.duckdb", read_only=True)
