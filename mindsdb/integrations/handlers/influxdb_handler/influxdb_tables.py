import pandas as pd

from typing import List

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.utilities.log import get_log

from mindsdb_sql.parser import ast
from mindsdb.integrations.handlers.utilities.query_utilities.select_query_utilities import SELECTQueryParser, SELECTQueryExecutor


logger = get_log("integrations.InfluxDB_handler")

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
        
        influxdb_tables_df = self.handler.call_influxdb_tables()
        select_statement_parser = SELECTQueryParser(
            query,
            self.handler.connection_data['influxdb_table_name'],
            self.get_columns(influxdb_tables_df)
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        select_statement_executor = SELECTQueryExecutor(
            influxdb_tables_df,
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit
        )
        influxdb_tables_df  = select_statement_executor.execute_query()

        

        return influxdb_tables_df

    def get_columns(self,dataframe) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses
        Returns
        -------
        List[str]
            List of columns
        """

        return list(dataframe.columns)
