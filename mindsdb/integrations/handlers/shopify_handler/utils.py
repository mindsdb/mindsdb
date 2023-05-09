import pandas as pd
from typing import Text, List, Dict, Tuple

from mindsdb_sql.parser import ast
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions


def parse_statement(
        query: ast.Select,
        table: Text,
        columns: List[Text]
) -> Tuple[List[Text], List[List[Text]], Dict[Text, List[Text]], int]:
    """
    Parses a SQL SELECT statement into its components: SELECT, WHERE, ORDER BY, LIMIT.
    """
    # SELECT
    selected_columns = []
    for target in query.targets:
        if isinstance(target, ast.Star):
            selected_columns = columns
            break
        elif isinstance(target, ast.Identifier):
            selected_columns.append(target.parts[-1])
        else:
            raise ValueError(f"Unknown query target {type(target)}")

    # WHERE
    where_conditions = extract_comparison_conditions(query.where)

    # ORDER BY
    order_by_conditions = {}
    if query.order_by and len(query.order_by) > 0:
        order_by_conditions["columns"] = []
        order_by_conditions["ascending"] = []

        for an_order in query.order_by:
            if an_order.field.parts[0] == table:
                if an_order.field.parts[1] in columns:
                    order_by_conditions["columns"].append(an_order.field.parts[1])

                    if an_order.direction == "ASC":
                        order_by_conditions["ascending"].append(True)
                    else:
                        order_by_conditions["ascending"].append(False)
                else:
                    raise ValueError(
                        f"Order by unknown column {an_order.field.parts[1]}"
                    )

    # LIMIT
    if query.limit:
        result_limit = query.limit.value
    else:
        result_limit = 20

    return selected_columns, where_conditions, order_by_conditions, result_limit


def get_results(
        df: pd.DataFrame,
        selected_columns: List[Text],
        where_conditions: List[List[Text]],
        order_by_conditions: Dict[Text, List[Text]],
        result_limit: int = None
) -> pd.DataFrame:
    """
    Get results from a DataFrame based on conditions extracted from a parsed query statement.
    """
    if result_limit:
        df = df.head(result_limit)

    if len(where_conditions) > 0:
        df = filter_df(df, where_conditions)

    if len(df) == 0:
        df = pd.DataFrame([], columns=selected_columns)
    else:
        df = df[selected_columns]

        if len(order_by_conditions.get("columns", [])) > 0:
            df = df.sort_values(
                by=order_by_conditions["columns"],
                ascending=order_by_conditions["ascending"],
            )

    return df


def filter_df(df: pd.DataFrame, where_conditions: List[List[Text]]) -> pd.DataFrame:
    """
    Filter a DataFrame based on a list of WHERE conditions.
    """
    for condition in where_conditions:
        column = condition[1]
        operator = '==' if condition[0] == '=' else condition[0]
        value = f"'{condition[2]}'" if type(condition[2]) == str else condition[2]

        query = f"{column} {operator} {value}"
        df.query(query, inplace=True)

    return df