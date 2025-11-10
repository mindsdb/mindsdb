from typing import Optional, List, Dict, Any

import pandas as pd
import mysql.connector

from mindsdb_sql_parser import parse_sql
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql_parser.ast.base import ASTNode

from mindsdb.utilities import log
from mindsdb.integrations.libs.base import MetaDatabaseHandler
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


def _make_table_response(result: List[Dict[str, Any]], cursor: mysql.connector.cursor.MySQLCursor) -> Response:
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


class MySQLHandler(MetaDatabaseHandler):
    """
    This handler handles connection and execution of the MySQL statements.
    """

    name = "mysql"

    def __init__(self, name: str, **kwargs: Any) -> None:
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = "mysql"
        self.connection_data = kwargs.get("connection_data", {})
        self.database = self.connection_data.get("database")

        self.connection: Optional[mysql.connector.MySQLConnection] = None

    def __del__(self) -> None:
        if self.is_connected:
            self.disconnect()

    def _unpack_config(self) -> Dict[str, Any]:
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
    def is_connected(self) -> bool:
        """
        Checks if the handler is connected to the MySQL database.

        Returns:
            bool: True if the handler is connected, False otherwise.
        """
        return self.connection is not None and self.connection.is_connected()

    @is_connected.setter
    def is_connected(self, value: bool) -> None:
        pass

    def connect(self) -> mysql.connector.MySQLConnection:
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

    def disconnect(self) -> None:
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
            logger.error(f"Error connecting to MySQL {self.connection_data.get('database', 'unknown')}! Error: {e}")
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
            logger.error(
                f"Error running query: {query} on {self.connection_data.get('database', 'unknown')}! Error: {e}"
            )
            response = Response(RESPONSE_TYPE.ERROR, error_code=e.errno or 1, error_message=str(e))
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

    def get_columns(self, table_name: str) -> Response:
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

    def meta_get_tables(self, table_names: Optional[List[str]] = None) -> Response:
        """
        Retrieves metadata information about the tables in the MySQL database
        to be stored in the data catalog.

        Args:
            table_names (list): A list of table names for which to retrieve metadata information.

        Returns:
            Response: A response object containing the metadata information.
        """
        query = """
            SELECT 
                t.TABLE_NAME as table_name,
                t.TABLE_SCHEMA as table_schema,
                t.TABLE_TYPE as table_type,
                t.TABLE_COMMENT as table_description,
                t.TABLE_ROWS as row_count
            FROM information_schema.TABLES t
            WHERE t.TABLE_SCHEMA = DATABASE()
                AND t.TABLE_TYPE IN ('BASE TABLE', 'VIEW')
        """

        if table_names is not None and len(table_names) > 0:
            quoted_names = [f"'{t}'" for t in table_names]
            query += f" AND t.TABLE_NAME IN ({','.join(quoted_names)})"

        query += " ORDER BY t.TABLE_NAME"

        result = self.native_query(query)
        return result

    def meta_get_columns(self, table_names: Optional[List[str]] = None) -> Response:
        """
        Retrieves column metadata for the specified tables (or all tables if no list is provided).

        Args:
            table_names (list): A list of table names for which to retrieve column metadata.

        Returns:
            Response: A response object containing the column metadata.
        """
        query = """
            SELECT 
                c.TABLE_NAME as table_name,
                c.COLUMN_NAME as column_name,
                c.DATA_TYPE as data_type,
                c.COLUMN_COMMENT as column_description,
                c.COLUMN_DEFAULT as column_default,
                CASE WHEN c.IS_NULLABLE = 'YES' THEN 1 ELSE 0 END as is_nullable
            FROM information_schema.COLUMNS c
            WHERE c.TABLE_SCHEMA = DATABASE()
        """

        if table_names is not None and len(table_names) > 0:
            quoted_names = [f"'{t}'" for t in table_names]
            query += f" AND c.TABLE_NAME IN ({','.join(quoted_names)})"

        query += " ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION"

        result = self.native_query(query)
        return result

    def meta_get_column_statistics(self, table_names: Optional[List[str]] = None) -> Response:
        """
        Retrieves column statistics for the specified tables (or all tables if no list is provided).
        Uses MySQL 8.0+ metadata sources (INFORMATION_SCHEMA.COLUMN_STATISTICS and INFORMATION_SCHEMA.STATISTICS) not requiring table scans.

        Args:
            table_names (list): A list of table names for which to retrieve column statistics.

        Returns:
            Response: A response object containing the column statistics.
        """
        table_filter = ""
        if table_names:
            quoted = ",".join(f"'{t}'" for t in table_names)
            table_filter = f" AND c.TABLE_NAME IN ({quoted})"

        query = f"""
            WITH cols AS (
                SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, c.ORDINAL_POSITION
                FROM information_schema.COLUMNS c
                WHERE c.TABLE_SCHEMA = DATABASE()
                {table_filter}
            ),
            hist AS (
                SELECT
                    cs.SCHEMA_NAME AS TABLE_SCHEMA,
                    cs.TABLE_NAME,
                    cs.COLUMN_NAME,
                    cs.HISTOGRAM,
                    JSON_LENGTH(cs.HISTOGRAM, '$.buckets') AS buckets_len
                FROM information_schema.COLUMN_STATISTICS cs
                WHERE cs.SCHEMA_NAME = DATABASE()
            ),
            ndv AS (
                SELECT
                    s.TABLE_SCHEMA,
                    s.TABLE_NAME,
                    s.COLUMN_NAME,
                    MAX(s.CARDINALITY) AS DISTINCT_VALUES_COUNT
                FROM information_schema.STATISTICS s
                WHERE s.TABLE_SCHEMA = DATABASE()
                GROUP BY s.TABLE_SCHEMA, s.TABLE_NAME, s.COLUMN_NAME
            )
            SELECT
                c.TABLE_NAME  AS TABLE_NAME,
                c.COLUMN_NAME AS COLUMN_NAME,

                /* optional fields kept NULL for simplicity */
                CAST(NULL AS JSON) AS MOST_COMMON_VALUES,
                CAST(NULL AS JSON) AS MOST_COMMON_FREQUENCIES,

                /* histogram "null-values" fraction -> percent */
                CASE
                WHEN h.HISTOGRAM IS NULL THEN NULL
                ELSE ROUND(
                        CAST(JSON_UNQUOTE(JSON_EXTRACT(h.HISTOGRAM, '$."null-values"')) AS DECIMAL(10,6)) * 100,
                        2
                    )
                END AS NULL_PERCENTAGE,
                /* MIN: first bucket's point (singleton) or lower endpoint (equi-height) */
                CASE
                WHEN h.HISTOGRAM IS NULL THEN NULL
                ELSE COALESCE(
                        JSON_UNQUOTE(JSON_EXTRACT(h.HISTOGRAM, '$.buckets[0].value')),
                        JSON_UNQUOTE(JSON_EXTRACT(h.HISTOGRAM, '$.buckets[0].endpoint[0]'))
                    )
                END AS MINIMUM_VALUE,

                /* MAX: last bucket's point (singleton) or upper endpoint (equi-height) */
                CASE
                WHEN h.HISTOGRAM IS NULL THEN NULL
                ELSE COALESCE(
                        JSON_UNQUOTE(
                        JSON_EXTRACT(h.HISTOGRAM,
                            CONCAT('$.buckets[', GREATEST(h.buckets_len - 1, 0), '].value')
                        )
                        ),
                        JSON_UNQUOTE(
                        JSON_EXTRACT(h.HISTOGRAM,
                            CONCAT('$.buckets[', GREATEST(h.buckets_len - 1, 0), '].endpoint[1]')
                        )
                        ),
                        JSON_UNQUOTE(
                        JSON_EXTRACT(h.HISTOGRAM,
                            CONCAT('$.buckets[', GREATEST(h.buckets_len - 1, 0), '].endpoint[0]')
                        )
                        )
                    )
                END AS MAXIMUM_VALUE,
                n.DISTINCT_VALUES_COUNT
            FROM cols c
            LEFT JOIN hist h
                ON h.TABLE_SCHEMA = c.TABLE_SCHEMA
                AND h.TABLE_NAME  = c.TABLE_NAME
                AND h.COLUMN_NAME = c.COLUMN_NAME
            LEFT JOIN ndv n
                ON n.TABLE_SCHEMA = c.TABLE_SCHEMA
                AND n.TABLE_NAME   = c.TABLE_NAME
                AND n.COLUMN_NAME  = c.COLUMN_NAME
            ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION;
        """
        return self.native_query(query)

    def meta_get_primary_keys(self, table_names: Optional[List[str]] = None) -> Response:
        """
        Retrieves primary key information for the specified tables (or all tables if no list is provided).

        Args:
            table_names (list): A list of table names for which to retrieve primary key information.

        Returns:
            Response: A response object containing the primary key information.
        """
        query = """
            SELECT 
                tc.TABLE_NAME as table_name,
                kcu.COLUMN_NAME as column_name,
                kcu.ORDINAL_POSITION as ordinal_position,
                tc.CONSTRAINT_NAME as constraint_name
            FROM information_schema.TABLE_CONSTRAINTS tc
            INNER JOIN information_schema.KEY_COLUMN_USAGE kcu
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                AND tc.TABLE_NAME = kcu.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                AND tc.TABLE_SCHEMA = DATABASE()
        """

        if table_names is not None and len(table_names) > 0:
            quoted_names = [f"'{t}'" for t in table_names]
            query += f" AND tc.TABLE_NAME IN ({','.join(quoted_names)})"

        query += " ORDER BY tc.TABLE_NAME, kcu.ORDINAL_POSITION"

        result = self.native_query(query)
        return result

    def meta_get_foreign_keys(self, table_names: Optional[List[str]] = None) -> Response:
        """
        Retrieves foreign key information for the specified tables (or all tables if no list is provided).

        Args:
            table_names (list): A list of table names for which to retrieve foreign key information.

        Returns:
            Response: A response object containing the foreign key information.
        """
        query = """
            SELECT 
                kcu.REFERENCED_TABLE_NAME as parent_table_name,
                kcu.REFERENCED_COLUMN_NAME as parent_column_name,
                kcu.TABLE_NAME as child_table_name,
                kcu.COLUMN_NAME as child_column_name,
                kcu.CONSTRAINT_NAME as constraint_name
            FROM information_schema.KEY_COLUMN_USAGE kcu
            WHERE kcu.TABLE_SCHEMA = DATABASE()
                AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
        """

        if table_names is not None and len(table_names) > 0:
            quoted_names = [f"'{t}'" for t in table_names]
            query += f" AND kcu.TABLE_NAME IN ({','.join(quoted_names)})"

        query += " ORDER BY kcu.TABLE_NAME, kcu.CONSTRAINT_NAME"

        result = self.native_query(query)
        return result
