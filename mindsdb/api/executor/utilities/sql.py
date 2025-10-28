import copy
from typing import List

import duckdb
from duckdb import InvalidInputException
import numpy as np
import orjson

from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser.ast import ASTNode, Select, Identifier, Function, Constant

from mindsdb.integrations.utilities.query_traversal import query_traversal
from mindsdb.utilities import log
from mindsdb.utilities.exception import QueryError
from mindsdb.utilities.functions import resolve_table_identifier, resolve_model_identifier
from mindsdb.utilities.json_encoder import CustomJSONEncoder
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.api.executor.utilities.mysql_to_duckdb_functions import mysql_to_duckdb_fnc

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

    Raises:
        QueryError: Raised when DuckDB fails to execute the query
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
    except InvalidInputException as e:
        raise QueryError(
            db_type="DuckDB",
            db_error_msg=f"DuckDB failed to execute query, likely due to inability to determine column data types. Details: {e}",
            failed_query=query_str,
            is_external=False,
            is_expected=False,
        ) from e
    except Exception as e:
        raise QueryError(
            db_type="DuckDB", db_error_msg=str(e), failed_query=query_str, is_external=False, is_expected=False
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
        raise QueryError(
            db_type="DuckDB",
            db_error_msg="Only 'SELECT from TABLE' statements supported for internal query",
            failed_query=query_str,
            is_external=False,
            is_expected=False,
        )

    query_ast.from_table.parts = ["df"]

    return query_dfs({"df": df}, query_ast, session=session)


def query_dfs(dataframes, query_ast, session=None):
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
            fnc = mysql_to_duckdb_fnc(node)
            if fnc is not None:
                node2 = fnc(node)
                if node2 is not None:
                    # copy alias
                    node2.alias = node.alias
                return node2

            fnc_name = node.op.lower()
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
                raise QueryError(
                    db_type="DuckDB",
                    db_error_msg=(
                        f"Unknown function: '{fnc_name}'. This function is not recognized during internal query processing.\n"
                        "Please use DuckDB-supported functions instead."
                    ),
                    failed_query=query_str,
                    is_external=False,
                    is_expected=False,
                )

    query_traversal(query_ast, adapt_query)

    def _convert(v):
        if isinstance(v, dict) or isinstance(v, list):
            try:
                default_encoder = CustomJSONEncoder().default
                return orjson.dumps(
                    v, default=default_encoder, option=orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_PASSTHROUGH_DATETIME
                ).decode("utf-8")
            except Exception:
                pass
        return v

    render = SqlalchemyRender("postgres")
    try:
        query_str = render.get_string(query_ast, with_failback=False)
    except Exception:
        logger.exception(f"Exception during query casting to 'postgres' dialect. Query:\n{str(query_ast)}.\nError:")
        query_str = render.get_string(query_ast, with_failback=True)

    for table_name, df in dataframes.items():
        for column in json_columns:
            df[column] = df[column].apply(_convert)

        if len(df) > 0:
            # workaround to prevent duckdb.TypeMismatchException
            for sys_name, sys_col in (
                ("models", "TRAINING_OPTIONS"),
                ("predictors", "TRAINING_OPTIONS"),
                ("ml_engines", "CONNECTION_DATA"),
            ):
                if table_name.lower() in sys_name and sys_col in df.columns:
                    df[sys_col] = df[sys_col].astype("string")

    result_df, description = query_df_with_type_infer_fallback(query_str, dataframes, user_functions=user_functions)
    result_df.replace({np.nan: None}, inplace=True)
    result_df.columns = [x[0] for x in description]
    return result_df
