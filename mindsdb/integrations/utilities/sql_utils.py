from enum import Enum
from typing import Any, Optional
import pandas as pd
import datetime as dt

from mindsdb.api.executor.utilities.sql import query_df
from mindsdb_sql_parser import ast
from mindsdb_sql_parser.ast.base import ASTNode

from mindsdb.integrations.utilities.query_traversal import query_traversal
from mindsdb.utilities.config import config
from mindsdb.utilities import log

logger = log.getLogger(__name__)


AGGREGATE_FUNCTIONS = {"count", "sum", "avg", "min", "max", "first", "last", "median"}


def is_aggregate_function(node: ast.ASTNode) -> bool:
    """Check if AST node is aggregate function

    Args:
        node: AST node to check

    Returns:
        bool: True if node is aggregate function
    """
    return isinstance(node, ast.Function) and hasattr(node, "op") and node.op.lower() in AGGREGATE_FUNCTIONS


def has_aggregate_function(targets: list[ast.ASTNode]) -> bool:
    """Check if any of AST node in the list is aggregate function

    Args:
        targets: list of AST nodes to check

    Returns:
        bool: True if node is aggregate function
    """
    return any(is_aggregate_function(target) for target in targets)


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


def extract_comparison_conditions(binary_op: ASTNode, ignore_functions=False, strict=True):
    """Extracts all simple comparison conditions that must be true from an AST node.
    Does NOT support 'or' conditions.
    """
    conditions = []
    captured_nodes = set()  # Track nodes we've captured as raw conditions to avoid processing their children

    def _extract_comparison_conditions(node: ASTNode, **kwargs):
        # Check if this node is a child of an already-captured node
        callstack = kwargs.get("callstack", [])
        for parent in callstack:
            if id(parent) in captured_nodes:
                # Skip processing children of captured nodes
                return

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
                if strict:
                    raise NotImplementedError(f"Not implemented arg1: {arg1}")
                else:
                    conditions.append(node)
                    captured_nodes.add(id(node))  # Mark this node as captured
                    return

            if isinstance(arg2, ast.Constant):
                value = arg2.value
            elif isinstance(arg2, ast.Tuple):
                value = [i.value for i in arg2.items]
            else:
                # When strict=False, return the entire node as a raw condition
                # (similar to how arg1 non-identifier case is handled)
                if strict:
                    raise NotImplementedError(f"Not implemented arg2: {arg2}")
                else:
                    conditions.append(node)
                    captured_nodes.add(id(node))  # Mark this node as captured
                    return

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


def _evaluate_interval_expression(node: ASTNode) -> Optional[dt.date]:
    """Evaluate INTERVAL expressions to get actual date values.

    Args:
        node: AST node that may contain INTERVAL expressions

    Returns:
        date if the expression can be evaluated, None otherwise
    """
    if isinstance(node, ast.BinaryOperation):
        op = node.op.lower()
        arg1, arg2 = node.args

        # Handle CURRENT_DATE - INTERVAL 'X day'
        if op == "-":
            # Check for CURRENT_DATE function
            if isinstance(arg1, ast.Function) and arg1.op.upper() == "CURRENT_DATE":
                if isinstance(arg2, ast.Interval):
                    interval_value = arg2.args[0] if arg2.args else None
                    # Handle both Constant node and direct string value
                    if interval_value:
                        if isinstance(interval_value, ast.Constant):
                            interval_str = str(interval_value.value).lower()
                        elif isinstance(interval_value, str):
                            interval_str = interval_value.lower()
                        else:
                            interval_str = str(interval_value).lower()

                        # Parse interval like '90 day', '30 days', etc.
                        import re

                        match = re.search(r"(\d+)\s*(day|days|d)", interval_str)
                        if match:
                            days = int(match.group(1))
                            result = dt.date.today() - dt.timedelta(days=days)
                            return result
            # Also handle if arg1 is CURRENT_DATE as an identifier/constant
            elif isinstance(arg1, ast.Identifier) and arg1.parts[-1].upper() == "CURRENT_DATE":
                if isinstance(arg2, ast.Interval):
                    interval_value = arg2.args[0] if arg2.args else None
                    # Handle both Constant node and direct string value
                    if interval_value:
                        if isinstance(interval_value, ast.Constant):
                            interval_str = str(interval_value.value).lower()
                        elif isinstance(interval_value, str):
                            interval_str = interval_value.lower()
                        else:
                            interval_str = str(interval_value).lower()

                        import re

                        match = re.search(r"(\d+)\s*(day|days|d)", interval_str)
                        if match:
                            days = int(match.group(1))
                            result = dt.date.today() - dt.timedelta(days=days)
                            return result

        # Handle CURRENT_DATE + INTERVAL 'X day'
        elif op == "+":
            if isinstance(arg1, ast.Function) and arg1.op.upper() == "CURRENT_DATE":
                if isinstance(arg2, ast.Interval):
                    interval_value = arg2.args[0] if arg2.args else None
                    if interval_value and isinstance(interval_value, ast.Constant):
                        interval_str = str(interval_value.value).lower()
                        import re

                        match = re.search(r"(\d+)\s*(day|days|d)", interval_str)
                        if match:
                            days = int(match.group(1))
                            return dt.date.today() + dt.timedelta(days=days)
            elif isinstance(arg1, ast.Identifier) and arg1.parts[-1].upper() == "CURRENT_DATE":
                if isinstance(arg2, ast.Interval):
                    interval_value = arg2.args[0] if arg2.args else None
                    if interval_value and isinstance(interval_value, ast.Constant):
                        interval_str = str(interval_value.value).lower()
                        import re

                        match = re.search(r"(\d+)\s*(day|days|d)", interval_str)
                        if match:
                            days = int(match.group(1))
                            return dt.date.today() + dt.timedelta(days=days)

    return None


def _extract_date_from_raw_condition(condition: ASTNode) -> Optional[tuple]:
    """Try to extract a date value and column from a raw condition for API pushdown.

    Args:
        condition: Raw condition AST node

    Returns:
        Tuple of (column_name, operator, date_value) if extractable, None otherwise
    """
    if isinstance(condition, ast.BinaryOperation):
        op = condition.op.lower()
        arg1, arg2 = condition.args

        # Handle CAST(column AS DATE) >= CURRENT_DATE - INTERVAL 'X day'
        if isinstance(arg1, ast.TypeCast):
            # type_name might be Identifier or string, handle both
            if isinstance(arg1.type_name, ast.Identifier):
                type_name_str = arg1.type_name.parts[-1].upper()
            else:
                type_name_str = str(arg1.type_name).upper()

            if type_name_str in ("DATE", "DATETIME", "TIMESTAMP"):
                if isinstance(arg1.arg, ast.Identifier):
                    column_name = arg1.arg.parts[-1]
                    date_value = _evaluate_interval_expression(arg2)
                    if date_value:
                        return (column_name, op, date_value)

        # Handle column >= CURRENT_DATE - INTERVAL 'X day'
        elif isinstance(arg1, ast.Identifier):
            column_name = arg1.parts[-1]
            date_value = _evaluate_interval_expression(arg2)
            if date_value:
                return (column_name, op, date_value)

    return None


def _is_date_expression_static(node):
    """Check if a node represents a date expression (CURRENT_DATE, INTERVAL, etc.)
    This is a static version that can be used outside filter_dataframe.
    """
    if isinstance(node, ast.BinaryOperation):
        op = node.op.lower()
        arg1, arg2 = node.args

        # Check for CURRENT_DATE - INTERVAL or CURRENT_DATE + INTERVAL
        if op in ("-", "+"):
            # Check if one side is CURRENT_DATE (function or identifier)
            is_current_date = False
            if isinstance(arg1, ast.Function) and arg1.op.upper() == "CURRENT_DATE":
                is_current_date = True
            elif isinstance(arg1, ast.Identifier) and arg1.parts[-1].upper() == "CURRENT_DATE":
                is_current_date = True

            # Check if other side is INTERVAL
            is_interval = isinstance(arg2, ast.Interval)

            if is_current_date and is_interval:
                return True

            # Also check if we can evaluate it (for validation)
            if _evaluate_interval_expression(node) is not None:
                return True
    elif isinstance(node, ast.Function) and node.op.upper() == "CURRENT_DATE":
        return True
    elif isinstance(node, ast.Identifier) and node.parts[-1].upper() == "CURRENT_DATE":
        return True
    return False


def _get_date_columns_from_raw_condition(condition: ASTNode) -> list:
    """Extract column names that should be cast to date/datetime from raw conditions.

    Args:
        condition: Raw condition AST node

    Returns:
        List of column names that need date casting
    """
    date_columns = []

    def _traverse_for_date_columns(node):
        if isinstance(node, ast.BinaryOperation):
            arg1, arg2 = node.args

            # Check if arg2 is a date expression using the same logic
            is_date_expr = _is_date_expression_static(arg2)

            # If comparing to a date expression, the column needs casting
            if is_date_expr:
                if isinstance(arg1, ast.Identifier):
                    date_columns.append(arg1.parts[-1])
                elif isinstance(arg1, ast.TypeCast) and isinstance(arg1.arg, ast.Identifier):
                    # Already has CAST, but ensure the column is cast in dataframe
                    date_columns.append(arg1.arg.parts[-1])

    _traverse_for_date_columns(condition)
    return date_columns


def filter_dataframe(df: pd.DataFrame, conditions: list, raw_conditions=None, order_by=None):
    # convert list of conditions to ast.
    # assumes that list was got from extract_comparison_conditions
    where_query = None
    date_columns_to_cast = set()

    for op, arg1, arg2 in conditions:
        op = op.lower()

        if op == "between":
            item = ast.BetweenOperation(args=[ast.Identifier(arg1), ast.Constant(arg2[0]), ast.Constant(arg2[1])])
        else:
            if isinstance(arg2, (tuple, list)):
                arg2 = ast.Tuple(arg2)

            # Check if arg2 is a date string (ISO format) and arg1 might be a date column
            # If so, we need to cast the column to timestamp for proper comparison
            arg1_identifier = ast.Identifier(arg1)
            if isinstance(arg2, str) and len(arg2) >= 10 and arg2[4] == "-" and arg2[7] == "-":
                # Looks like a date string (YYYY-MM-DD format)
                # Check if the column exists and might be a date column
                if arg1 in df.columns:
                    # Cast column to TIMESTAMP for proper date comparison
                    cast_column = ast.TypeCast(arg=arg1_identifier, type_name="TIMESTAMP")
                    # Cast the date string to TIMESTAMP as well
                    cast_value = ast.TypeCast(arg=ast.Constant(arg2), type_name="TIMESTAMP")
                    item = ast.BinaryOperation(op=op, args=[cast_column, cast_value])
                    date_columns_to_cast.add(arg1)
                else:
                    item = ast.BinaryOperation(op=op, args=[arg1_identifier, ast.Constant(arg2)])
            else:
                item = ast.BinaryOperation(op=op, args=[arg1_identifier, ast.Constant(arg2)])

        if where_query is None:
            where_query = item
        else:
            where_query = ast.BinaryOperation(op="and", args=[where_query, item])

    # Process raw conditions - detect date columns and cast them, also add CAST to condition if needed
    processed_raw_conditions = []
    # date_columns_to_cast is already initialized above, continue using it

    for condition in raw_conditions or []:
        # Extract date columns that need casting
        date_cols = _get_date_columns_from_raw_condition(condition)
        date_columns_to_cast.update(date_cols)

        # If condition has a column without CAST but comparing to date, add CAST
        if isinstance(condition, ast.BinaryOperation):
            arg1, arg2 = condition.args

            # Check if arg2 is a date expression
            is_date_expr = _is_date_expression_static(arg2)

            # If comparing to date expression and column doesn't have CAST, add it
            if is_date_expr and isinstance(arg1, ast.Identifier):
                # Wrap column in CAST to TIMESTAMP
                # TypeCast expects type_name as a string (not Identifier)
                cast_column = ast.TypeCast(arg=arg1, type_name="TIMESTAMP")
                condition = ast.BinaryOperation(op=condition.op, args=[cast_column, arg2])

        processed_raw_conditions.append(condition)

    # Cast date columns in dataframe (from both raw conditions and regular conditions)
    for col_name in date_columns_to_cast:
        if col_name in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df[col_name]):
                try:
                    df[col_name] = pd.to_datetime(df[col_name], errors="coerce")
                except Exception as e:
                    logger.warning(f"[SQL Utils] Failed to cast column {col_name} to datetime: {e}")
                    pass  # If casting fails, let DuckDB handle it via CAST in query

    if processed_raw_conditions:
        for condition in processed_raw_conditions:
            if where_query is None:
                where_query = condition
            else:
                where_query = ast.BinaryOperation(op="and", args=[where_query, condition])

    query = ast.Select(targets=[ast.Star()], from_table=ast.Identifier("df"), where=where_query)

    if order_by:
        query.order_by = order_by

    result = query_df(df, query)
    if len(result) == 0 and len(df) > 0:
        logger.warning(
            f"[SQL Utils] Filter returned 0 rows from {len(df)} input rows - this might indicate a filtering issue"
        )
    return result


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
