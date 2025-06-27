import pandas as pd

from typing import List

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.utilities import log

from mindsdb_sql_parser import ast
from mindsdb.integrations.utilities.handlers.query_utilities.select_query_utilities import SELECTQueryParser


logger = log.getLogger(__name__)


class InfluxDBTables(APITable):
    """InfluxDB Tables implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the InfluxDB "query" API endpoint
        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query
        Returns
        -------
        pd.DataFrame of particular InfluxDB table matching the query
        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        table_name = self.handler.connection_data['influxdb_table_name']
        select_statement_parser = SELECTQueryParser(
            query,
            "tables",
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, _ = select_statement_parser.parse_query()

        try:
            selected_columns.remove("name")
            selected_columns.remove("tags")
        except Exception as e:
            logger.warn(e)

        formatted_query = self.get_select_query(table_name, selected_columns, where_conditions, order_by_conditions, query.limit)
        influxdb_tables_df = self.handler.call_influxdb_tables(formatted_query)

        return influxdb_tables_df

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses
        Returns
        -------
        List[str]
            List of columns
        """

        dataframe = self.handler.call_influxdb_tables(f"SELECT * FROM {self.handler.connection_data['influxdb_table_name']} LIMIT 1")

        return list(dataframe.columns)

    def get_select_query(self, table_name, selected_columns, where_conditions, order_by_conditions, result_limit):
        """Gets Well formed Query
        Returns
        -------
        str
        """
        columns = ", ".join([f'"{column}"' for column in selected_columns])
        query = f'SELECT {columns} FROM "{table_name}"'
        if (where_conditions is not None and len(where_conditions) > 0):
            query += " WHERE "
            query += " AND ".join([f"{i[1]} {i[0]} {i[2]}" for i in where_conditions])
        if (order_by_conditions != {} and order_by_conditions['columns'] is not None and len(order_by_conditions['columns']) > 0):
            query += " ORDER BY "
            query += ", ".join([f'{column_name} {"ASC"if asc else "DESC"}' for column_name, asc in zip(order_by_conditions['columns'], order_by_conditions['ascending'])])
        if (result_limit is not None):
            query += f" LIMIT {result_limit}"
        query += ";"
        return query
