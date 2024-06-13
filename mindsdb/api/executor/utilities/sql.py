import copy
from typing import List

import duckdb
from duckdb import InvalidInputException
import numpy as np

from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql.planner.utils import query_traversal
from mindsdb_sql.parser.ast import (
    ASTNode, Select, Identifier,
    Function, Constant
)
from mindsdb.utilities.functions import resolve_table_identifier, resolve_model_identifier

from mindsdb.utilities import log
from mindsdb.utilities.json_encoder import CustomJSONEncoder

logger = log.getLogger(__name__)


def _get_query_tables(query: ASTNode, resolve_function: callable, default_database: str = None) -> List[tuple]:
    """Find all tables/models in the query

    Args:
        query (ASTNode): query
        resolve_function (callable): function apply to identifier
        default_database (str): database name that will be used if there is no db name in identifier

    Returns:
        List[tuple]: list with (db/project name, table name, version)
    """
    tables = []

    def _get_tables(node, is_table, **kwargs):
        if is_table and isinstance(node, Identifier):
            table = resolve_function(node)
            if table[0] is None:
                table = (default_database,) + table[1:]
            tables.append(table)

    query_traversal(query, _get_tables)
    return tables


def get_query_tables(query: ASTNode, default_database: str = None) -> List[tuple]:
    return _get_query_tables(query, resolve_table_identifier, default_database)


def get_query_models(query: ASTNode, default_database: str = None) -> List[tuple]:
    return _get_query_tables(query, resolve_model_identifier, default_database)


def query_df_with_type_infer_fallback(query_str: str, dataframes: dict, user_functions=None):
    ''' Duckdb need to infer column types if column.dtype == object. By default it take 1000 rows,
        but that may be not sufficient for some cases. This func try to run query multiple times
        increasing butch size for type infer

        Args:
            query_str (str): query to execute
            dataframes (dict): dataframes
            user_functions: functions controller which register new functions in connection

        Returns:
            pandas.DataFrame
            pandas.columns
    '''

    for name, value in dataframes.items():
        locals()[name] = value

    con = duckdb.connect(database=':memory:')
    if user_functions:
        user_functions.register(con)

    for sample_size in [1000, 10000, 1000000]:
        try:
            con.execute(f'set global pandas_analyze_sample={sample_size};')
            result_df = con.execute(query_str).fetchdf()
        except InvalidInputException:
            pass
        else:
            break
    else:
        raise InvalidInputException
    description = con.description
    con.close()

    return result_df, description


def query_df(df, query, session=None):
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

    if isinstance(query_ast, Select) is False \
       or isinstance(query_ast.from_table, Identifier) is False:
        raise Exception(
            "Only 'SELECT from TABLE' statements supported for internal query"
        )

    table_name = query_ast.from_table.parts[0]
    query_ast.from_table.parts = ['df']

    json_columns = set()

    if session is not None:
        user_functions = session.function_controller.create_function_set()
    else:
        user_functions = None

    def adapt_query(node, is_table, **kwargs):
        if is_table:
            return
        if isinstance(node, Identifier):
            if len(node.parts) > 1:
                node.parts = [node.parts[-1]]
                return node
        if isinstance(node, Function):
            fnc_name = node.op.lower()
            if fnc_name == 'database' and len(node.args) == 0:
                if session is not None:
                    cur_db = session.database
                else:
                    cur_db = None
                return Constant(cur_db)
            elif fnc_name == 'truncate':
                # replace mysql 'truncate' function to duckdb 'round'
                node.op = 'round'
                if len(node.args) == 1:
                    node.args.append(0)
            elif fnc_name == 'json_extract':
                json_columns.add(node.args[0].parts[-1])
            else:
                if user_functions is not None:
                    user_functions.check_function(node)

    query_traversal(query_ast, adapt_query)

    # convert json columns
    encoder = CustomJSONEncoder()

    def _convert(v):
        if isinstance(v, dict) or isinstance(v, list):
            try:
                return encoder.encode(v)
            except Exception:
                pass
        return v
    for column in json_columns:
        df[column] = df[column].apply(_convert)

    render = SqlalchemyRender('postgres')
    try:
        query_str = render.get_string(query_ast, with_failback=False)
    except Exception as e:
        logger.error(
            f"Exception during query casting to 'postgres' dialect. Query: {str(query)}. Error: {e}"
        )
        query_str = render.get_string(query_ast, with_failback=True)

    # workaround to prevent duckdb.TypeMismatchException
    if len(df) > 0:
        if table_name.lower() in ('models', 'predictors'):
            if 'TRAINING_OPTIONS' in df.columns:
                df = df.astype({'TRAINING_OPTIONS': 'string'})
        if table_name.lower() == 'ml_engines':
            if 'CONNECTION_DATA' in df.columns:
                df = df.astype({'CONNECTION_DATA': 'string'})

    result_df, description = query_df_with_type_infer_fallback(query_str, {'df': df}, user_functions=user_functions)
    result_df = result_df.replace({np.nan: None})

    new_column_names = {}
    real_column_names = [x[0] for x in description]
    for i, duck_column_name in enumerate(result_df.columns):
        new_column_names[duck_column_name] = real_column_names[i]
    result_df = result_df.rename(
        new_column_names,
        axis='columns'
    )
    return result_df
