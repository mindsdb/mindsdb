import pandas as pd
from typing import Text, List, Dict, Tuple

from mindsdb_sql.parser import ast
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions


class SELECTQueryParser:
    """
    Parses a SELECT query into its component parts.

    Parameters
    ----------
    query : ast.Select
        Given SQL SELECT query.
    table : Text
        Name of the table to query.
    columns : List[Text]
        List of columns in the table.
    """
    def __init__(self, query: ast.Select, table: Text, columns: List[Text]):
        self.query = query
        self.table = table
        self.columns = columns

    def parse_query(self) -> Tuple[List[Text], List[List[Text]], Dict[Text, List[Text]], int]:
        """
        Parses a SQL SELECT statement into its components: SELECT, WHERE, ORDER BY, LIMIT.
        """
        selected_columns = self.parse_select_clause()
        where_conditions = self.parse_where_clause()
        order_by_conditions = self.parse_order_by_clause()
        result_limit = self.parse_limit_clause()

        return selected_columns, where_conditions, order_by_conditions, result_limit

    def parse_select_clause(self) -> List[Text]:
        """
        Parses the SELECT (column selection) clause of the query.
        """
        selected_columns = []
        for target in self.query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.columns
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        return selected_columns

    def parse_where_clause(self) -> List[List[Text]]:
        """
        Parses the WHERE clause of the query.
        """
        where_conditions = extract_comparison_conditions(self.query.where)
        return where_conditions

    def parse_order_by_clause(self) -> Dict[Text, List[Text]]:
        """
        Parses the ORDER BY clause of the query.
        """
        order_by_conditions = {}
        if self.query.order_by and len(self.query.order_by) > 0:
            order_by_conditions["columns"] = []
            order_by_conditions["ascending"] = []

            for an_order in self.query.order_by:
                if an_order.field.parts[0] == self.table:
                    if an_order.field.parts[1] in self.columns:
                        order_by_conditions["columns"].append(an_order.field.parts[1])

                        if an_order.direction == "ASC":
                            order_by_conditions["ascending"].append(True)
                        else:
                            order_by_conditions["ascending"].append(False)
                    else:
                        raise ValueError(
                            f"Order by unknown column {an_order.field.parts[1]}"
                        )

        return order_by_conditions

    def parse_limit_clause(self) -> int:
        """
        Parses the LIMIT clause of the query.
        """
        if self.query.limit:
            result_limit = self.query.limit.value
        else:
            result_limit = 20

        return result_limit


class SELECTQueryExecutor:
    """
    Executes a SELECT query.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe to query.
    selected_columns : List[Text]
        List of columns to select.
    where_conditions : List[List[Text]]
        List of where conditions.
    order_by_conditions : Dict[Text, List[Text]]
        Dictionary of order by conditions.
    result_limit : int
        Number of results to return.
    """
    def __init__(self, df: pd.DataFrame, selected_columns: List[Text], where_conditions: List[List[Text]], order_by_conditions: Dict[Text, List[Text]], result_limit: int = None):
        self.df = df
        self.selected_columns = selected_columns
        self.where_conditions = where_conditions
        self.order_by_conditions = order_by_conditions
        self.result_limit = result_limit

    def execute_query(self):
        """
        Execute the query.
        """
        self.execute_limit_clause()

        self.execute_where_clause()

        self.execute_select_clause()

        self.execute_order_by_clause()

        return self.df

    def execute_select_clause(self):
        """
        Execute the select clause of the query.
        """
        if len(self.df) == 0:
            self.df = pd.DataFrame([], columns=self.selected_columns)
        else:
            self.df = self.df[self.selected_columns]

    def execute_where_clause(self):
        """
        Execute the where clause of the query.
        """
        if len(self.where_conditions) > 0:
            for condition in self.where_conditions:
                column = condition[1]
                operator = '==' if condition[0] == '=' else condition[0]
                value = f"'{condition[2]}'" if type(condition[2]) == str else condition[2]

                query = f"{column} {operator} {value}"
                self.df.query(query, inplace=True)

    def execute_order_by_clause(self):
        """
        Execute the order by clause of the query.
        """
        if len(self.order_by_conditions.get("columns", [])) > 0:
            self.df = self.df.sort_values(
                by=self.order_by_conditions["columns"],
                ascending=self.order_by_conditions["ascending"],
            )

    def execute_limit_clause(self):
        """
        Execute the limit clause of the query.
        """
        if self.result_limit:
            self.df = self.df.head(self.result_limit)