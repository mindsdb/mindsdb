from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController
from mindsdb_sql.parser import ast
from mindsdb_sql.parser.ast import Identifier, Constant, BinaryOperation
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.planner.utils import query_traversal


def make_sql_session():
    sql_session = SessionController()
    sql_session.database = 'mindsdb'
    return sql_session


def get_where_data(where):
    result = {}
    if type(where) != BinaryOperation:
        raise Exception("Wrong 'where' statement")
    if where.op == '=':
        if type(where.args[0]) != Identifier or type(where.args[1]) != Constant:
            raise Exception("Wrong 'where' statement")
        result[where.args[0].parts[-1]] = where.args[1].value
    elif where.op == 'and':
        result.update(get_where_data(where.args[0]))
        result.update(get_where_data(where.args[1]))
    else:
        raise Exception("Wrong 'where' statement")
    return result


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
            elif not isinstance(node.args[0], ast.Identifier) or not isinstance(node.args[1], ast.Constant):
                # Only support [identifier] =/</>/>=/<=/etc [constant] comparisons.
                raise NotImplementedError
            conditions.append([op, node.args[0].parts[-1], node.args[1].value])

    query_traversal(binary_op, _extract_comparison_conditions)
    return conditions
