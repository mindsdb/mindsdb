import copy
from typing import List

import duckdb
from duckdb import InvalidInputException
import numpy as np

from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser.ast import ASTNode, Select, Identifier, Function, Constant

from mindsdb.integrations.utilities.query_traversal import query_traversal
from mindsdb.utilities import log
from mindsdb.utilities.exception import format_db_error_message
from mindsdb.utilities.functions import resolve_table_identifier, resolve_model_identifier
from mindsdb.utilities.json_encoder import CustomJSONEncoder
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.api.executor.utilities.mysql_to_duckdb_functions import (
    adapt_char_fn,
    adapt_locate_fn,
    adapt_unhex_fn,
    adapt_format_fn,
    adapt_sha2_fn,
    adapt_length_fn,
    adapt_regexp_substr_fn,
    adapt_substring_index_fn,
    adapt_curtime_fn,
    adapt_timestampdiff_fn,
    adapt_extract_fn,
)

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
    """Duckdb need to infer column types if column.dtype == object. By default it take 1000 rows,
    but that may be not sufficient for some cases. This func try to run query multiple times
    increasing butch size for type infer

    Args:
        query_str (str): query to execute
        dataframes (dict): dataframes
        user_functions: functions controller which register new functions in connection

    Returns:
        pandas.DataFrame
        pandas.columns
    """

    try:
        with duckdb.connect(database=":memory:") as con:
            if user_functions:
                user_functions.register(con)

            for name, value in dataframes.items():
                con.register(name, value)

            exception = None
            for sample_size in [1000, 10000, 1000000]:
                try:
                    con.execute(f"set global pandas_analyze_sample={sample_size};")
                    result_df = con.execute(query_str).fetchdf()
                except InvalidInputException as e:
                    exception = e
                else:
                    break
            else:
                raise exception
            description = con.description
    except Exception as e:
        raise Exception(
            format_db_error_message(db_type="DuckDB", db_error_msg=str(e), failed_query=query_str, is_external=False)
        ) from e

    return result_df, description


_duckdb_functions_and_kw_list = None


def get_duckdb_functions_and_kw_list() -> list[str] | None:
    """Returns a list of all functions and keywords supported by DuckDB.
    The list is merge of:
     - list of duckdb's functions: 'select * from duckdb_functions()' or 'pragma functions'
     - ist of keywords, because of some functions are just sintax-sugar
       and not present in the duckdb_functions (like 'if()').
     - hardcoded list of window_functions, because there are no way to get if from duckdb,
       and they are not present in the duckdb_functions()

    Returns:
        list[str] | None: List of supported functions and keywords, or None if unable to retrieve the list.
    """
    global _duckdb_functions_and_kw_list
    window_functions_list = [
        "cume_dist",
        "dense_rank",
        "first_value",
        "lag",
        "last_value",
        "lead",
        "nth_value",
        "ntile",
        "percent_rank",
        "rank_dense",
        "rank",
        "row_number",
    ]
    if _duckdb_functions_and_kw_list is None:
        try:
            df, _ = query_df_with_type_infer_fallback(
                """
                select distinct name
                from (
                    select function_name as name from duckdb_functions()
                    union all
                    select keyword_name as name from duckdb_keywords()
                ) ta;
            """,
                dataframes={},
            )
            df.columns = [name.lower() for name in df.columns]
            _duckdb_functions_and_kw_list = df["name"].drop_duplicates().str.lower().to_list() + window_functions_list
        except Exception as e:
            logger.warning(f"Unable to get DuckDB functions list: {e}")

    return _duckdb_functions_and_kw_list


def query_df(df, query, session=None):
    """Perform simple query ('select' from one table, without subqueries and joins) on DataFrame.

    Args:
        df (pandas.DataFrame): data
        query (mindsdb_sql_parser.ast.Select | str): select query

    Returns:
        pandas.DataFrame
    """

    if isinstance(query, str):
        query_ast = parse_sql(query)
        query_str = query
    else:
        query_ast = copy.deepcopy(query)
        query_str = str(query)

    if isinstance(query_ast, Select) is False or isinstance(query_ast.from_table, Identifier) is False:
        raise Exception("Only 'SELECT from TABLE' statements supported for internal query")

    table_name = query_ast.from_table.parts[0]
    query_ast.from_table.parts = ["df"]

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

            mysql_to_duck_fn_map = {
                "char": adapt_char_fn,
                "locate": adapt_locate_fn,
                "insrt": adapt_locate_fn,
                "unhex": adapt_unhex_fn,
                "format": adapt_format_fn,
                "sha2": adapt_sha2_fn,
                "length": adapt_length_fn,
                "regexp_substr": adapt_regexp_substr_fn,
                "substring_index": adapt_substring_index_fn,
                "curtime": adapt_curtime_fn,
                "timestampdiff": adapt_timestampdiff_fn,
                "extract": adapt_extract_fn,
            }
            if fnc_name in mysql_to_duck_fn_map:
                return mysql_to_duck_fn_map[fnc_name](node)

            if fnc_name == "database" and len(node.args) == 0:
                if session is not None:
                    cur_db = session.database
                else:
                    cur_db = None
                return Constant(cur_db)
            elif fnc_name == "truncate":
                # replace mysql 'truncate' function to duckdb 'round'
                node.op = "round"
                if len(node.args) == 1:
                    node.args.append(0)
            elif fnc_name == "json_extract":
                json_columns.add(node.args[0].parts[-1])
            else:
                if user_functions is not None:
                    user_functions.check_function(node)

            duckdb_functions_and_kw_list = get_duckdb_functions_and_kw_list() or []
            custom_functions_list = [] if user_functions is None else list(user_functions.functions.keys())
            all_functions_list = duckdb_functions_and_kw_list + custom_functions_list
            if len(all_functions_list) > 0 and fnc_name not in all_functions_list:
                raise Exception(
                    format_db_error_message(
                        db_type="DuckDB",
                        db_error_msg=(
                            f"Unknown function: '{fnc_name}'. This function is not recognized during internal query processing.\n"
                            "Please use DuckDB-supported functions instead."
                        ),
                        failed_query=query_str,
                        is_external=False,
                    )
                )

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

    render = SqlalchemyRender("postgres")
    try:
        query_str = render.get_string(query_ast, with_failback=False)
    except Exception as e:
        logger.error(f"Exception during query casting to 'postgres' dialect. Query: {str(query)}. Error: {e}")
        query_str = render.get_string(query_ast, with_failback=True)

    # workaround to prevent duckdb.TypeMismatchException
    if len(df) > 0:
        if table_name.lower() in ("models", "predictors"):
            if "TRAINING_OPTIONS" in df.columns:
                df = df.astype({"TRAINING_OPTIONS": "string"})
        if table_name.lower() == "ml_engines":
            if "CONNECTION_DATA" in df.columns:
                df = df.astype({"CONNECTION_DATA": "string"})

    result_df, description = query_df_with_type_infer_fallback(query_str, {"df": df}, user_functions=user_functions)
    result_df.replace({np.nan: None}, inplace=True)
    result_df.columns = [x[0] for x in description]
    return result_df
