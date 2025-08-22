from enum import Enum
from typing import Any
import pandas as pd

from mindsdb.api.executor.utilities.sql import query_df
from mindsdb_sql_parser import ast
from mindsdb_sql_parser.ast.base import ASTNode

from mindsdb.integrations.utilities.query_traversal import query_traversal
from mindsdb.utilities.config import config


class FilterOperator(Enum):
    """
    Enum for filter operators.
    """

    EQUAL = "="
    NOT_EQUAL = "!="
    LESS_THAN = "<"
    LESS_THAN_OR_EQUAL = "<="
    GREATER_THAN = ">"
    GREATER_THAN_OR_EQUAL = ">="
    IN = "IN"
    NOT_IN = "NOT IN"
    BETWEEN = "BETWEEN"
    NOT_BETWEEN = "NOT BETWEEN"
    LIKE = "LIKE"
    NOT_LIKE = "NOT LIKE"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"
    IS = "IS"
    IS_NOT = "IS NOT"


class FilterCondition:
    """
    Base class for filter conditions.
    """

    def __init__(self, column: str, op: FilterOperator, value: Any):
        self.column = column
        self.op = op
        self.value = value
        self.applied = False

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, FilterCondition):
            return self.column == __value.column and self.op == __value.op and self.value == __value.value
        else:
            return False

    def __repr__(self) -> str:
        return f"""
            FilterCondition(
                column={self.column},
                op={self.op},
                value={self.value}
            )
        """


class KeywordSearchArgs:
    def __init__(self, column: str, query: str):
        """
        Args:
            column: The column to search in.
            query: The search query string.
        """
        self.column = column
        self.query = query


class SortColumn:
    def __init__(self, column: str, ascending: bool = True):
        self.column = column
        self.ascending = ascending
        self.applied = False


def make_sql_session():
    from mindsdb.api.executor.controllers.session_controller import SessionController

    sql_session = SessionController()
    sql_session.database = config.get("default_project")
    return sql_session


def conditions_to_filter(binary_op: ASTNode):
    conditions = extract_comparison_conditions(binary_op)

    filters = {}
    for op, arg1, arg2 in conditions:
        if op != "=":
            raise NotImplementedError
        filters[arg1] = arg2
    return filters


def extract_comparison_conditions(binary_op: ASTNode, ignore_functions=False):
    """Extracts all simple comparison conditions that must be true from an AST node.
    Does NOT support 'or' conditions.
    """
    conditions = []

    def _extract_comparison_conditions(node: ASTNode, **kwargs):
        if isinstance(node, ast.BinaryOperation):
            op = node.op.lower()
            if op == "and":
                # Want to separate individual conditions, not include 'and' as its own condition.
                return

            arg1, arg2 = node.args
            if ignore_functions and isinstance(arg1, ast.Function):
                # handle lower/upper
                if arg1.op.lower() in ("lower", "upper"):
                    if isinstance(arg1.args[0], ast.Identifier):
                        arg1 = arg1.args[0]

            if not isinstance(arg1, ast.Identifier):
                # Only support [identifier] =/</>/>=/<=/etc [constant] comparisons.
                raise NotImplementedError(f"Not implemented arg1: {arg1}")

            if isinstance(arg2, ast.Constant):
                value = arg2.value
            elif isinstance(arg2, ast.Tuple):
                value = [i.value for i in arg2.items]
            else:
                raise NotImplementedError(f"Not implemented arg2: {arg2}")

            conditions.append([op, arg1.parts[-1], value])
        if isinstance(node, ast.BetweenOperation):
            var, up, down = node.args
            if not (
                isinstance(var, ast.Identifier) and isinstance(up, ast.Constant) and isinstance(down, ast.Constant)
            ):
                raise NotImplementedError(f"Not implemented: {node}")

            op = node.op.lower()
            conditions.append([op, var.parts[-1], (up.value, down.value)])

    query_traversal(binary_op, _extract_comparison_conditions)
    return conditions


def project_dataframe(df, targets, table_columns):
    """
    case-insensitive projection
    'select A' and 'select a' return different column case but with the same content
    """

    columns = []
    df_cols_idx = {col.lower(): col for col in df.columns}
    df_col_rename = {}

    for target in targets:
        if isinstance(target, ast.Star):
            for col in table_columns:
                col_df = df_cols_idx.get(col.lower())
                if col_df is not None:
                    df_col_rename[col_df] = col
                columns.append(col)

            break
        elif isinstance(target, ast.Identifier):
            col = target.parts[-1]
            col_df = df_cols_idx.get(col.lower())
            if col_df is not None:
                if hasattr(target, "alias") and isinstance(target.alias, ast.Identifier):
                    df_col_rename[col_df] = target.alias.parts[0]
                else:
                    df_col_rename[col_df] = col
            columns.append(col)
        else:
            raise NotImplementedError

    if len(df) == 0:
        df = pd.DataFrame([], columns=columns)
    else:
        # add absent columns
        for col in set(columns) & set(df.columns) ^ set(columns):
            df[col] = None

        # filter by columns
        df = df[columns]

    # adapt column names to projection
    if len(df_col_rename) > 0:
        df.rename(columns=df_col_rename, inplace=True)
    return df


def filter_dataframe(df: pd.DataFrame, conditions: list):
    # convert list of conditions to ast.
    # assumes that list was got from extract_comparison_conditions
    where_query = None
    for op, arg1, arg2 in conditions:
        op = op.lower()

        if op == "between":
            item = ast.BetweenOperation(args=[ast.Identifier(arg1), ast.Constant(arg2[0]), ast.Constant(arg2[1])])
        else:
            if isinstance(arg2, (tuple, list)):
                arg2 = ast.Tuple(arg2)

            item = ast.BinaryOperation(op=op, args=[ast.Identifier(arg1), ast.Constant(arg2)])
        if where_query is None:
            where_query = item
        else:
            where_query = ast.BinaryOperation(op="and", args=[where_query, item])

    query = ast.Select(targets=[ast.Star()], from_table=ast.Identifier("df"), where=where_query)

    return query_df(df, query)


def sort_dataframe(df, order_by: list):
    cols = []
    ascending = []
    for order in order_by:
        if not isinstance(order, ast.OrderBy):
            continue

        col = order.field.parts[-1]
        if col not in df.columns:
            continue

        cols.append(col)
        ascending.append(False if order.direction.lower() == "desc" else True)
    if len(cols) > 0:
        df = df.sort_values(by=cols, ascending=ascending)
    return df
