import copy
import duckdb
import numpy as np
from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import Select, Identifier, BinaryOperation, OrderBy
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender

from mindsdb.utilities.log import log


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

    if isinstance(query, str):
        query_ast = parse_sql(query, dialect='mysql')
    else:
        query_ast = copy.deepcopy(query)

    if isinstance(query_ast, Select) is False or isinstance(query_ast.from_table, Identifier) is False:
        raise Exception("Only 'SELECT from TABLE' statements supported for internal query")

    query_ast.from_table.parts = ['df_table']
    for identifier in query_ast.targets:
        if isinstance(identifier, Identifier):
            identifier.parts = [identifier.parts[-1]]
    if isinstance(query_ast.order_by, list):
        for orderby in query_ast.order_by:
            if isinstance(orderby, OrderBy) and isinstance(orderby.field, Identifier):
                orderby.field.parts = [orderby.field.parts[-1]]
    _remove_table_name(query_ast.where)

    render = SqlalchemyRender('postgres')
    try:
        query_str = render.get_string(query_ast, with_failback=False)
    except Exception as e:
        log.error(f"Exception during query casting to 'postgres' dialect. Query: {str(query)}. Error: {e}")
        query_str = render.get_string(query_ast, with_failback=True)

    res = duckdb.query_df(df, 'df_table', query_str)
    result_df = res.df()
    result_df = result_df.replace({np.nan: None})
    return result_df
