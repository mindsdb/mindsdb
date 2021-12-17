from uuid import uuid4

import duckdb
from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import Select, Join, Identifier, BinaryOperation, Constant, Operation, UnaryOperation, OrderBy
from mindsdb_sql.parser.ast.select.star import Star


def get_alias(element):
    if '.'.join(element.parts) == '*':
        return '*'
    return '.'.join(element.parts) if element.alias is None else element.alias


def identifier_to_dict(identifier):
    res = {
        'value': '.'.join(identifier.parts),
        'name': get_alias(identifier)
    }
    return res


def where_to_dict(root):
    if isinstance(root, BinaryOperation):
        op = root.op.lower()
        if op == '=':
            op = 'eq'
        return {op: [where_to_dict(root.args[0]), where_to_dict(root.args[1])]}
    elif isinstance(root, UnaryOperation):
        op = root.op.lower()
        return {op: [where_to_dict(root.args[0])]}
    elif isinstance(root, Identifier):
        return root.value
    elif isinstance(root, Constant):
        if isinstance(root.value, str):
            return {'literal': root.value}
        else:
            return root.value
    else:
        raise Exception(f'unknown type in "where": {root}')


def _remove_table_name(root):
    if isinstance(root, BinaryOperation):
        _remove_table_name(root.args[0])
        _remove_table_name(root.args[1])
    elif isinstance(root, Identifier):
        root.parts = [root.parts[-1]]


def query_df(df, query):
    """ Perform simple query ('select' from one table, without subqueries and joins) on DataFrame.

        Args:
            df (pandas.DataFrame): data
            query (mindsdb_sql.parser.ast.Select | str): select query

        Returns:
            pandas.DataFrame
    """

    query = parse_sql(str(query), dialect='mysql')
    if isinstance(query, Select) is False or isinstance(query.from_table, Identifier) is False:
        raise Exception("Only 'SELECT from TABLE' statements supported for internal query")

    query.from_table.parts = ['df_table']
    for identifier in query.targets:
        if isinstance(identifier, Identifier):
            identifier.parts = [identifier.parts[-1]]
    if isinstance(query.order_by, list):
        for orderby in query.order_by:
            if isinstance(orderby, OrderBy) and isinstance(orderby.field, Identifier):
                orderby.field.parts = [orderby.field.parts[-1]]
    _remove_table_name(query.where)

    res = duckdb.query_df(df, 'df_table', query)
    result_df = res.df()
    return result_df
