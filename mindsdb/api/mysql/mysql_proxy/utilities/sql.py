import duckdb
import pandas as pd
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

    render = SqlalchemyRender('postgres')
    try:
        query_str = render.get_string(query, with_failback=False)
    except Exception as e:
        log.error(f"Exception during query casting to 'postgres' dialect. Query: {query}. Error: {e}")
        query_str = render.get_string(query, with_failback=True)

    res = duckdb.query_df(df, 'df_table', query_str)
    result_df = res.df()
    result_df = result_df.where(pd.notnull(result_df), None)
    return result_df
