import pandas as pd

from mindsdb_sql.parser import ast
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.planner.utils import query_traversal


def make_sql_session():
    from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController

    sql_session = SessionController()
    sql_session.database = 'mindsdb'
    return sql_session


def conditions_to_filter(binary_op: ASTNode):
    conditions = extract_comparison_conditions(binary_op)

    filters = {}
    for op, arg1, arg2 in conditions:
        if op != '=':
            raise NotImplementedError
        filters[arg1] = arg2
    return filters


def extract_comparison_conditions(binary_op: ASTNode):
    '''Extracts all simple comparison conditions that must be true from an AST node.
    Does NOT support 'or' conditions.
    '''
    conditions = []

    def _extract_comparison_conditions(node: ASTNode, **kwargs):
        if isinstance(node, ast.BinaryOperation):
            op = node.op.lower()
            if op == 'and':
                # Want to separate individual conditions, not include 'and' as its own condition.
                return
            elif not isinstance(node.args[0], ast.Identifier):
                # Only support [identifier] =/</>/>=/<=/etc [constant] comparisons.
                raise NotImplementedError(f'Not implemented arg1: {node.args[0]}')

            if isinstance(node.args[1], ast.Constant):
                value = node.args[1].value
            elif isinstance(node.args[1], ast.Tuple):
                value = [i.value for i in node.args[1].items]
            else:
                raise NotImplementedError(f'Not implemented arg2: {node.args[1]}')

            conditions.append([op, node.args[0].parts[-1], value])

    query_traversal(binary_op, _extract_comparison_conditions)
    return conditions


def project_dataframe(df, targets, table_columns):
    columns = []
    for target in targets:
        if isinstance(target, ast.Star):
            columns = table_columns
            break
        elif isinstance(target, ast.Identifier):
            columns.append(target.parts[-1])
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
    return df