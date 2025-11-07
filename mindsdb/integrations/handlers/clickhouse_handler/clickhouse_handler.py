from urllib.parse import quote, urlencode
from typing import Optional, List

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from clickhouse_sqlalchemy.drivers.base import ClickHouseDialect
from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender

from mindsdb.utilities import log
from mindsdb.integrations.libs.base import MetaDatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)

logger = log.getLogger(__name__)


class ClickHouseHandler(MetaDatabaseHandler):
    """
    This handler handles connection and execution of the ClickHouse statements.
    """

    name = "clickhouse"

    def __init__(self, name, connection_data, **kwargs):
        super().__init__(name)
        self.dialect = "clickhouse"
        self.connection_data = connection_data
        self.renderer = SqlalchemyRender(ClickHouseDialect)
        self.is_connected = False
        self.protocol = connection_data.get('protocol', 'native')
        self._has_is_nullable_column = None  # Cache for version check

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self):
        """
        Establishes a connection to a ClickHouse server using SQLAlchemy.

        Raises:
            SQLAlchemyError: If an error occurs while connecting to the database.

        Returns:
            Connection: A SQLAlchemy Connection object to the ClickHouse database.
        """
        if self.is_connected:
            return self.connection

        protocol = "clickhouse+native" if self.protocol == "native" else "clickhouse+http"
        host = quote(self.connection_data["host"])
        port = self.connection_data["port"]
        user = quote(self.connection_data["user"])
        password = quote(self.connection_data["password"])
        database = quote(self.connection_data["database"])
        verify = self.connection_data.get("verify", True)
        url = f"{protocol}://{user}:{password}@{host}:{port}/{database}"
        # This is not redundunt. Check https://clickhouse-sqlalchemy.readthedocs.io/en/latest/connection.html#http

        params = {}
        if self.protocol == "https":
            params["protocol"] = "https"
        if verify is False:
            params["verify"] = "false"
        if params:
            url = f"{url}?{urlencode(params)}"

        try:
            engine = create_engine(url)
            connection = engine.raw_connection()
            self.is_connected = True
            self.connection = connection
        except SQLAlchemyError as e:
            logger.error(f"Error connecting to ClickHouse {self.connection_data['database']}, {e}!")
            self.is_connected = False
            raise

        return self.connection

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the ClickHouse.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        need_to_close = not self.is_connected

        try:
            connection = self.connect()
            cur = connection.cursor()
            try:
                cur.execute("select 1;")
            finally:
                cur.close()
            response.success = True
        except SQLAlchemyError as e:
            logger.error(f"Error connecting to ClickHouse {self.connection_data['database']}, {e}!")
            response.error_message = str(e)
            self.is_connected = False

        if response.success is True and need_to_close:
            self.disconnect()

        return response

    def native_query(self, query: str) -> Response:
        """
        Executes a SQL query and returns the result.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """

        connection = self.connect()
        cur = connection.cursor()
        try:
            cur.execute(query)
            result = cur.fetchall()
            if result:
                response = Response(RESPONSE_TYPE.TABLE, pd.DataFrame(result, columns=[x[0] for x in cur.description]))
            else:
                response = Response(RESPONSE_TYPE.OK)
            connection.commit()
        except SQLAlchemyError as e:
            logger.error(f"Error running query: {query} on {self.connection_data['database']}!")
            response = Response(RESPONSE_TYPE.ERROR, error_message=str(e))
            connection.rollback()
        finally:
            cur.close()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement with eliminated rows that dont satisfy the WHERE condition
        """
        query_str = self.renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Get a list with all of the tabels in ClickHouse db
        """
        q = f"SHOW TABLES FROM {self.connection_data['database']}"
        result = self.native_query(q)
        df = result.data_frame

        if df is not None:
            result.data_frame = df.rename(columns={df.columns[0]: "table_name"})

        return result

    def get_columns(self, table_name) -> Response:
        """
        Show details about the table
        """
        q = f"DESCRIBE {table_name}"
        result = self.native_query(q)
        return result

    def _check_has_is_nullable_column(self) -> bool:
        """
        Checks if the is_nullable column exists in system.columns table.
        This column was added in ClickHouse 23.x.
        
        Returns:
            bool: True if is_nullable column exists, False otherwise.
        """
        if self._has_is_nullable_column is not None:
            return self._has_is_nullable_column
        
        try:
            check_query = """
                SELECT name 
                FROM system.columns 
                WHERE database = 'system' 
                    AND table = 'columns' 
                    AND name = 'is_nullable'
            """
            result = self.native_query(check_query)
            self._has_is_nullable_column = (
                result.resp_type == RESPONSE_TYPE.TABLE 
                and not result.data_frame.empty
            )
        except Exception as e:
            logger.warning(f"Could not check for is_nullable column: {e}")
            self._has_is_nullable_column = False
        
        return self._has_is_nullable_column

    def meta_get_tables(self, table_names: Optional[List[str]] = None) -> Response:
        """
        Retrieves metadata information about the tables in the ClickHouse database
        to be stored in the data catalog.

        Args:
            table_names (list): A list of table names for which to retrieve metadata information.

        Returns:
            Response: A response object containing the metadata information.
        """
        database = self.connection_data['database']
        
        query = f"""
            SELECT 
                name as table_name,
                database as table_schema,
                engine as table_type,
                comment as table_description,
                total_rows as row_count
            FROM system.tables
            WHERE database = '{database}'
        """

        if table_names is not None and len(table_names) > 0:
            quoted_names = [f"'{t}'" for t in table_names]
            query += f" AND name IN ({','.join(quoted_names)})"

        query += " ORDER BY name"

        result = self.native_query(query)
        return result

    def meta_get_columns(self, table_names: Optional[List[str]] = None) -> Response:
        """
        Retrieves column metadata for the specified tables (or all tables if no list is provided).
        This includes column comments that you can set in ClickHouse using:
        ALTER TABLE table_name MODIFY COLUMN column_name Type COMMENT 'description'

        Args:
            table_names (list): A list of table names for which to retrieve column metadata.

        Returns:
            Response: A response object containing the column metadata.
        """
        database = self.connection_data['database']
        
        # Check if is_nullable column is available (ClickHouse 23.x+)
        has_is_nullable = self._check_has_is_nullable_column()
        
        # Build the SELECT clause based on available columns
        select_clause = """
                table as table_name,
                name as column_name,
                type as data_type,
                comment as column_description,
                default_expression as column_default"""
        
        if has_is_nullable:
            select_clause += """,
                is_nullable as is_nullable"""
        
        query = f"""
            SELECT {select_clause}
            FROM system.columns
            WHERE database = '{database}'
        """

        if table_names is not None and len(table_names) > 0:
            quoted_names = [f"'{t}'" for t in table_names]
            query += f" AND table IN ({','.join(quoted_names)})"

        query += " ORDER BY table, position"

        result = self.native_query(query)
        return result

    def meta_get_column_statistics(self, table_names: Optional[List[str]] = None) -> Response:
        """
        Retrieves column statistics for the specified tables (or all tables if no list is provided).
        Uses the base class implementation which calls meta_get_column_statistics_for_table for each table.

        Args:
            table_names (list): A list of table names for which to retrieve column statistics.

        Returns:
            Response: A response object containing the column statistics.
        """
        # Use the base class implementation that calls meta_get_column_statistics_for_table
        return super().meta_get_column_statistics(table_names)

    def meta_get_column_statistics_for_table(
        self, table_name: str, column_names: Optional[List[str]] = None
    ) -> Response:
        """
        Retrieves column statistics for a specific table.
        
        Args:
            table_name (str): The name of the table.
            column_names (Optional[List[str]]): List of column names to retrieve statistics for. 
                                                  If None, statistics for all columns will be returned.
        Returns:
            Response: A response object containing the column statistics for the table.
        """
        database = self.connection_data['database']

        # Get the list of columns for this table
        columns_query = f"""
            SELECT name, type
            FROM system.columns
            WHERE database = '{database}' AND table = '{table_name}'
        """

        if column_names:
            quoted_names = [f"'{c}'" for c in column_names]
            columns_query += f" AND name IN ({','.join(quoted_names)})"

        try:
            columns_result = self.native_query(columns_query)

            if columns_result.resp_type == RESPONSE_TYPE.ERROR or columns_result.data_frame.empty:
                logger.warning(f"No columns found for table {table_name}")
                return Response(RESPONSE_TYPE.TABLE, pd.DataFrame())

            # Build statistics query - collect all stats in one query
            select_parts = []
            for _, row in columns_result.data_frame.iterrows():
                col = row['name']
                # Use backticks to handle special characters in column names
                select_parts.extend([
                    f"countIf(`{col}` IS NULL) AS nulls_{col}",
                    f"uniq(`{col}`) AS distincts_{col}",
                    f"toString(min(`{col}`)) AS min_{col}",
                    f"toString(max(`{col}`)) AS max_{col}",
                ])

            if not select_parts:
                return Response(RESPONSE_TYPE.TABLE, pd.DataFrame())

            # Build the query to get stats for all columns at once
            stats_query = f"""
                SELECT 
                    count(*) AS total_rows,
                    {', '.join(select_parts)}
                FROM `{database}`.`{table_name}`
            """

            stats_result = self.native_query(stats_query)

            if stats_result.resp_type != RESPONSE_TYPE.TABLE or stats_result.data_frame.empty:
                logger.warning(f"Could not retrieve stats for table {table_name}")
                # Return placeholder stats
                placeholder_data = []
                for _, row in columns_result.data_frame.iterrows():
                    placeholder_data.append({
                        'table_name': table_name,
                        'column_name': row['name'],
                        'null_percentage': None,
                        'distinct_values_count': None,
                        'most_common_values': None,
                        'most_common_frequencies': None,
                        'minimum_value': None,
                        'maximum_value': None,
                    })
                return Response(RESPONSE_TYPE.TABLE, pd.DataFrame(placeholder_data))

            # Parse the stats result
            stats_data = stats_result.data_frame.iloc[0]
            total_rows = stats_data.get('total_rows', 0)

            # Build the final statistics DataFrame
            all_stats = []
            for _, row in columns_result.data_frame.iterrows():
                col = row['name']
                nulls = stats_data.get(f'nulls_{col}', 0)
                distincts = stats_data.get(f'distincts_{col}', None)
                min_val = stats_data.get(f'min_{col}', None)
                max_val = stats_data.get(f'max_{col}', None)

                # Calculate null percentage
                null_pct = None
                if total_rows is not None and total_rows > 0:
                    null_pct = round((nulls / total_rows) * 100, 2)

                all_stats.append({
                    'table_name': table_name,
                    'column_name': col,
                    'null_percentage': null_pct,
                    'distinct_values_count': distincts,
                    'most_common_values': None,
                    'most_common_frequencies': None,
                    'minimum_value': min_val,
                    'maximum_value': max_val,
                })

            return Response(RESPONSE_TYPE.TABLE, pd.DataFrame(all_stats))

        except Exception as e:
            logger.error(f"Exception while fetching statistics for table {table_name}: {e}")
            # Return empty stats on error
            return Response(
                RESPONSE_TYPE.ERROR, 
                error_message=f"Could not retrieve statistics for table {table_name}: {str(e)}"
            )

    def meta_get_primary_keys(self, table_names: Optional[List[str]] = None) -> Response:
        """
        Retrieves primary key information for the specified tables (or all tables if no list is provided).

        Args:
            table_names (list): A list of table names for which to retrieve primary key information.

        Returns:
            Response: A response object containing the primary key information.
        """
        database = self.connection_data['database']
        
        query = f"""
            SELECT 
                table as table_name,
                name as column_name,
                position as ordinal_position,
                'PRIMARY' as constraint_name
            FROM system.columns
            WHERE database = '{database}'
                AND is_in_primary_key = 1
        """

        if table_names is not None and len(table_names) > 0:
            quoted_names = [f"'{t}'" for t in table_names]
            query += f" AND table IN ({','.join(quoted_names)})"

        query += " ORDER BY table, position"

        result = self.native_query(query)
        return result

    def meta_get_foreign_keys(self, table_names: Optional[List[str]] = None) -> Response:
        """
        Retrieves foreign key information for the specified tables (or all tables if no list is provided).
        Note: ClickHouse does not enforce foreign key constraints, but this method is provided for completeness.

        Args:
            table_names (list): A list of table names for which to retrieve foreign key information.

        Returns:
            Response: A response object containing an empty DataFrame (ClickHouse doesn't support foreign keys).
        """
        # ClickHouse does not support foreign key constraints
        # Return an empty DataFrame with the expected columns
        df = pd.DataFrame(columns=[
            'parent_table_name',
            'parent_column_name', 
            'child_table_name',
            'child_column_name',
            'constraint_name'
        ])
        return Response(RESPONSE_TYPE.TABLE, df)

    def meta_get_handler_info(self, **kwargs) -> str:
        """
        Retrieves information about the ClickHouse handler design and implementation.

        Returns:
            str: A string containing information about the ClickHouse handler's capabilities.
        """
        return (
            "ClickHouse is a fast open-source column-oriented database management system.\n"
            "Key features:\n"
            "- Supports standard SQL syntax with some extensions\n"
            "- Use backticks (`) to quote table and column names with special characters\n"
            "- Does NOT support traditional foreign key constraints (they are not enforced)\n"
            "- Optimized for analytical queries (OLAP) rather than transactional operations (OLTP)\n"
            "- Supports various table engines (MergeTree, ReplacingMergeTree, SummingMergeTree, etc.)\n"
            "- Native support for arrays, nested structures, and approximate algorithms\n"
        )
