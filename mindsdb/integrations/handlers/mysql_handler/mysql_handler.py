import pandas as pd
import mysql.connector

from mindsdb_sql_parser import parse_sql
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql_parser.ast.base import ASTNode

from mindsdb.utilities import log
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)
from mindsdb.integrations.handlers.mysql_handler.settings import ConnectionConfig
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import C_TYPES, DATA_C_TYPE_MAP

logger = log.getLogger(__name__)


def _map_type(mysql_type_text: str) -> MYSQL_DATA_TYPE:
    """Map MySQL text types names to MySQL types as enum.

    Args:
        mysql_type_text (str): The name of the MySQL type to map.

    Returns:
        MYSQL_DATA_TYPE: The MySQL type enum that corresponds to the MySQL text type name.
    """
    try:
        return MYSQL_DATA_TYPE(mysql_type_text.upper())
    except Exception:
        logger.warning(f"MySQL handler: unknown type: {mysql_type_text}, use TEXT as fallback.")
        return MYSQL_DATA_TYPE.TEXT


def _make_table_response(result: list[dict], cursor: mysql.connector.cursor.MySQLCursor) -> Response:
    """Build response from result and cursor.

    Args:
        result (list[dict]): result of the query.
        cursor (mysql.connector.cursor.MySQLCursor): cursor object.

    Returns:
        Response: response object.
    """
    description = cursor.description
    reverse_c_type_map = {v.code: k for k, v in DATA_C_TYPE_MAP.items() if v.code != C_TYPES.MYSQL_TYPE_BLOB}
    mysql_types: list[MYSQL_DATA_TYPE] = []
    for col in description:
        type_int = col[1]
        if isinstance(type_int, int) is False:
            mysql_types.append(MYSQL_DATA_TYPE.TEXT)
            continue

        if type_int == C_TYPES.MYSQL_TYPE_TINY:
            # There are 3 types that returns as TINYINT: TINYINT, BOOL, BOOLEAN.
            mysql_types.append(MYSQL_DATA_TYPE.TINYINT)
            continue

        if type_int in reverse_c_type_map:
            mysql_types.append(reverse_c_type_map[type_int])
            continue

        if type_int == C_TYPES.MYSQL_TYPE_BLOB:
            # region determine text/blob type by flags
            # Unfortunately, there is no way to determine particular type of text/blob column by flags.
            # Subtype have to be determined by 8-s element of description tuple, but mysql.conector
            # return the same value for all text types (TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT), and for
            # all blob types (TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB).
            if col[7] == 16:  # and col[8] == 45
                mysql_types.append(MYSQL_DATA_TYPE.TEXT)
            elif col[7] == 144:  # and col[8] == 63
                mysql_types.append(MYSQL_DATA_TYPE.BLOB)
            else:
                logger.debug(f"MySQL handler: unknown type code {col[7]}, use TEXT as fallback.")
                mysql_types.append(MYSQL_DATA_TYPE.TEXT)
            # endregion
        else:
            logger.warning(f"MySQL handler: unknown type id={type_int} in column {col[0]}, use TEXT as fallback.")
            mysql_types.append(MYSQL_DATA_TYPE.TEXT)

    # region cast int and bool to nullable types
    serieses = []
    for i, mysql_type in enumerate(mysql_types):
        expected_dtype = None
        column_name = description[i][0]
        if mysql_type in (
            MYSQL_DATA_TYPE.SMALLINT,
            MYSQL_DATA_TYPE.INT,
            MYSQL_DATA_TYPE.MEDIUMINT,
            MYSQL_DATA_TYPE.BIGINT,
            MYSQL_DATA_TYPE.TINYINT,
        ):
            expected_dtype = "Int64"
        elif mysql_type in (MYSQL_DATA_TYPE.BOOL, MYSQL_DATA_TYPE.BOOLEAN):
            expected_dtype = "boolean"
        serieses.append(pd.Series([row[column_name] for row in result], dtype=expected_dtype, name=description[i][0]))
    df = pd.concat(serieses, axis=1, copy=False)
    # endregion

    response = Response(RESPONSE_TYPE.TABLE, df, affected_rows=cursor.rowcount, mysql_types=mysql_types)
    return response


class MySQLHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the MySQL statements.
    """

    name = "mysql"

    def __init__(self, name, **kwargs):
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = "mysql"
        self.connection_data = kwargs.get("connection_data", {})
        self.database = self.connection_data.get("database")

        self.connection = None

    def __del__(self):
        if self.is_connected:
            self.disconnect()

    def _unpack_config(self):
        """
        Unpacks the config from the connection_data by validation all parameters.

        Returns:
            dict: A dictionary containing the validated connection parameters.
        """
        try:
            config = ConnectionConfig(**self.connection_data)
            return config.model_dump(exclude_unset=True)
        except ValueError as e:
            raise ValueError(str(e))

    @property
    def is_connected(self):
        """
        Checks if the handler is connected to the MySQL database.

        Returns:
            bool: True if the handler is connected, False otherwise.
        """
        return self.connection is not None and self.connection.is_connected()

    @is_connected.setter
    def is_connected(self, value):
        pass

    def connect(self):
        """
        Establishes a connection to a MySQL database.

        Returns:
            MySQLConnection: An active connection to the database.
        """
        if self.is_connected and self.connection.is_connected():
            return self.connection
        config = self._unpack_config()
        if "conn_attrs" in self.connection_data:
            config["conn_attrs"] = self.connection_data["conn_attrs"]

        if "connection_timeout" not in config:
            config["connection_timeout"] = 10

        ssl = self.connection_data.get("ssl")
        if ssl is True:
            ssl_ca = self.connection_data.get("ssl_ca")
            ssl_cert = self.connection_data.get("ssl_cert")
            ssl_key = self.connection_data.get("ssl_key")
            config["client_flags"] = [mysql.connector.constants.ClientFlag.SSL]
            if ssl_ca is not None:
                config["ssl_ca"] = ssl_ca
            if ssl_cert is not None:
                config["ssl_cert"] = ssl_cert
            if ssl_key is not None:
                config["ssl_key"] = ssl_key
        elif ssl is False:
            config["ssl_disabled"] = True

        if "collation" not in config:
            config["collation"] = "utf8mb4_general_ci"
        if "use_pure" not in config:
            config["use_pure"] = True
        try:
            connection = mysql.connector.connect(**config)
            connection.autocommit = True
            self.connection = connection
            return self.connection
        except mysql.connector.Error as e:
            logger.error(f"Error connecting to MySQL {self.database}, {e}!")
            raise

    def disconnect(self):
        """
        Closes the connection to the MySQL database if it's currently open.
        """
        if self.is_connected is False:
            return
        self.connection.close()
        return

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the MySQL database.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """

        result = StatusResponse(False)
        need_to_close = not self.is_connected

        try:
            connection = self.connect()
            result.success = connection.is_connected()
        except mysql.connector.Error as e:
            logger.error(f"Error connecting to MySQL {self.connection_data['database']}, {e}!")
            result.error_message = str(e)

        if result.success and need_to_close:
            self.disconnect()

        return result

    def native_query(self, query: str) -> Response:
        """
        Executes a SQL query on the MySQL database and returns the result.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """

        need_to_close = not self.is_connected
        connection = None
        try:
            connection = self.connect()
            with connection.cursor(dictionary=True, buffered=True) as cur:
                cur.execute(query)
                if cur.with_rows:
                    result = cur.fetchall()
                    response = _make_table_response(result, cur)
                else:
                    response = Response(RESPONSE_TYPE.OK, affected_rows=cur.rowcount)
        except mysql.connector.Error as e:
            logger.error(f"Error running query: {query} on {self.connection_data['database']}!")
            response = Response(RESPONSE_TYPE.ERROR, error_message=str(e))
            if connection is not None and connection.is_connected():
                connection.rollback()

        if need_to_close:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement.
        """
        renderer = SqlalchemyRender("mysql")
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Get a list with all of the tabels in MySQL selected database
        """
        sql = """
            SELECT
                TABLE_SCHEMA AS table_schema,
                TABLE_NAME AS table_name,
                TABLE_TYPE AS table_type
            FROM
                information_schema.TABLES
            WHERE
                TABLE_TYPE IN ('BASE TABLE', 'VIEW')
                AND TABLE_SCHEMA = DATABASE()
            ORDER BY 2
            ;
        """
        result = self.native_query(sql)
        return result

    def get_columns(self, table_name) -> Response:
        """
        Show details about the table
        """
        q = f"""
            select
                COLUMN_NAME,
                DATA_TYPE,
                ORDINAL_POSITION,
                COLUMN_DEFAULT,
                IS_NULLABLE,
                CHARACTER_MAXIMUM_LENGTH,
                CHARACTER_OCTET_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                DATETIME_PRECISION,
                CHARACTER_SET_NAME,
                COLLATION_NAME
            from
                information_schema.columns
            where
                table_name = '{table_name}';
        """
        result = self.native_query(q)
        result.to_columns_table_response(map_type_fn=_map_type)
        return result
