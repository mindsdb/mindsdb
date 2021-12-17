from uuid import uuid4

import duckdb

from mindsdb_sql.parser.ast import Join, Identifier, BinaryOperation, Constant, Operation, UnaryOperation
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


def query_df(df, query):
    """ Perform simple query ('select' from one table, without subqueries and joins) on DataFrame.
        NOTE: current version of duckdb save tables in memory between connection, therefore for
        safetywe change name of table in query to radom string.

        Args:
            df (pandas.DataFrame): data
            query (mindsdb_sql.parser.ast.Select): select query

        Returns:
            pandas.DataFrame
    """
    uuid = f"x{str(uuid4()).replace('-', '')}"
    original_table_name = query.from_table.parts
    query.from_table.parts = [uuid]
    table_name = uuid
    query_str = str(query)
    query.from_table.parts = original_table_name

    con = duckdb.connect(database=':memory:')
    con.register(table_name, df)
    result_df = con.execute(query_str).fetchdf()
    con.unregister(table_name)
    con.close()
    return result_df
