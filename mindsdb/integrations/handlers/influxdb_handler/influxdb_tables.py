import pandas as pd

from typing import List

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.utilities import log

from mindsdb_sql.parser import ast

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
        conditions = extract_comparison_conditions(query.where)

       
        order_by_conditions = {}

        if query.order_by and len(query.order_by) > 0:
            order_by_conditions["columns"] = []
            order_by_conditions["ascending"] = []

            for an_order in query.order_by:
                if an_order.field.parts[0] != "":
                    next    
                if an_order.field.parts[1] in self.get_columns():
                    order_by_conditions["columns"].append(an_order.field.parts[1])

                    if an_order.direction == "ASC":
                        order_by_conditions["ascending"].append(True)
                    else:
                        order_by_conditions["ascending"].append(False)
                else:
                    raise ValueError(
                        f"Order by unknown column {an_order.field.parts[1]}"
                    )

        influxdb_tables_df = self.handler.call_influxdb_tables()

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")


        if len(influxdb_tables_df) == 0:
            influxdb_tables_df = pd.DataFrame([], columns=selected_columns)
        else:
            influxdb_tables_df.columns = self.get_columns()
            for col in set(influxdb_tables_df.columns).difference(set(selected_columns)):
                influxdb_tables_df = influxdb_tables_df.drop(col, axis=1)

            if len(order_by_conditions.get("columns", [])) > 0:
                influxdb_tables_df = influxdb_tables_df.sort_values(
                    by=order_by_conditions["columns"],
                    ascending=order_by_conditions["ascending"],
                )

        if query.limit:
            influxdb_tables_df = influxdb_tables_df.head(query.limit.value)

        return influxdb_tables_df

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses
        Returns
        -------
        List[str]
            List of columns
        """
        influxdb_df_fields = self.handler.call_influxdb_tables()

        return list(influxdb_df_fields.columns)
