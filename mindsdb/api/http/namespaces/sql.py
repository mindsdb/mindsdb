import time
import csv
import json
from http import HTTPStatus
from collections import defaultdict
from io import StringIO

from flask import request, Response
from flask_restx import Resource

from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser import ast

import mindsdb.utilities.hooks as hooks
import mindsdb.utilities.profiler as profiler
from mindsdb.api.http.utils import http_error
from mindsdb.api.http.namespaces.configs.sql import ns_conf
from mindsdb.api.mysql.mysql_proxy.mysql_proxy import SQLAnswer
from mindsdb.api.mysql.mysql_proxy.classes.fake_mysql_proxy import FakeMysqlProxy
from mindsdb.api.executor.data_types.response_type import (
    RESPONSE_TYPE as SQL_RESPONSE_TYPE,
)

from mindsdb.integrations.utilities.query_traversal import query_traversal
from mindsdb.api.executor.exceptions import ExecutorException, UnknownError
from mindsdb.metrics.metrics import api_endpoint_metrics
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.exception import QueryError
from mindsdb.utilities.functions import mark_process

logger = log.getLogger(__name__)


@ns_conf.route("/query")
@ns_conf.param("query", "Execute query")
class Query(Resource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @ns_conf.doc("query")
    @api_endpoint_metrics("POST", "/sql/query")
    @mark_process(name="http_query")
    def post(self):
        start_time = time.time()
        query = request.json["query"]
        context = request.json.get("context", {})
        params = request.json.get("params", {})

        if "params" in request.json:
            ctx.params = request.json["params"]
        if isinstance(query, str) is False or isinstance(context, dict) is False:
            return http_error(HTTPStatus.BAD_REQUEST, "Wrong arguments", 'Please provide "query" with the request.')
        logger.debug(f"Incoming query: {query}")

        if context.get("profiling") is True:
            profiler.enable()

        error_type = None
        error_code = None
        error_text = None
        error_traceback = None

        profiler.set_meta(query=query, api="http", environment=Config().get("environment"))
        with profiler.Context("http_query_processing"):
            mysql_proxy = FakeMysqlProxy()
            mysql_proxy.set_context(context)
            try:
                result: SQLAnswer = mysql_proxy.process_query(query, params=params)
                query_response: dict = result.dump_http_response()
            except ExecutorException as e:
                # classified error
                error_type = "expected"
                query_response = {
                    "type": SQL_RESPONSE_TYPE.ERROR,
                    "error_code": 0,
                    "error_message": str(e),
                }
                logger.warning(f"Error query processing: {e}")
            except QueryError as e:
                error_type = "expected" if e.is_expected else "unexpected"
                query_response = {
                    "type": SQL_RESPONSE_TYPE.ERROR,
                    "error_code": 0,
                    "error_message": str(e),
                }
                if e.is_expected:
                    logger.warning(f"Query failed due to expected reason: {e}")
                else:
                    logger.exception("Error query processing:")
            except UnknownError as e:
                # unclassified
                error_type = "unexpected"
                query_response = {
                    "type": SQL_RESPONSE_TYPE.ERROR,
                    "error_code": 0,
                    "error_message": str(e),
                }
                logger.exception("Error query processing:")

            except Exception as e:
                error_type = "unexpected"
                query_response = {
                    "type": SQL_RESPONSE_TYPE.ERROR,
                    "error_code": 0,
                    "error_message": str(e),
                }
                logger.exception("Error query processing:")

            if query_response.get("type") == SQL_RESPONSE_TYPE.ERROR:
                error_type = "expected"
                error_code = query_response.get("error_code")
                error_text = query_response.get("error_message")

            context = mysql_proxy.get_context()

            query_response["context"] = context

        hooks.after_api_query(
            company_id=ctx.company_id,
            api="http",
            command=None,
            payload=query,
            error_type=error_type,
            error_code=error_code,
            error_text=error_text,
            traceback=error_traceback,
        )

        end_time = time.time()
        log_msg = f"SQL processed in {(end_time - start_time):.2f}s ({end_time:.2f}-{start_time:.2f}), result is {query_response['type']}"
        if query_response["type"] is SQL_RESPONSE_TYPE.TABLE:
            log_msg += f" ({len(query_response['data'])} rows), "
        elif query_response["type"] is SQL_RESPONSE_TYPE.ERROR:
            log_msg += f" ({query_response['error_message']}), "
        log_msg += f"used handlers {ctx.used_handlers}"
        logger.debug(log_msg)

        return query_response, 200


@ns_conf.route("/query/utils/parametrize_constants")
class ParametrizeConstants(Resource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @api_endpoint_metrics("POST", "/query/utils/parametrize_constants")
    def post(self):
        sql_query = request.json["query"]

        # find constants in the query and replace them with parameters
        query = parse_sql(sql_query)

        parameters = []
        param_counts = {}
        databases = defaultdict(set)

        def to_parameter(param_name, value):
            if param_name is None:
                param_name = default_param_name

            num = param_counts.get(param_name, 1)
            param_counts[param_name] = num + 1

            if num > 1:
                param_name = param_name + str(num)

            parameters.append({"name": param_name, "value": value, "type": type(value).__name__})
            return ast.Parameter(param_name)

        def find_constants_f(node, is_table, is_target, callstack, **kwargs):
            if is_table and isinstance(node, ast.Identifier):
                if len(node.parts) > 1:
                    databases[node.parts[0]].add(".".join(node.parts[1:]))

            if not isinstance(node, ast.Constant):
                return

            # it is a target
            if is_target and node.alias is not None:
                return to_parameter(node.alias.parts[-1], node.value)

            param_name = None

            for item in callstack:
                # try to find the name
                if isinstance(item, (ast.BinaryOperation, ast.BetweenOperation)) and item.op.lower() not in (
                    "and",
                    "or",
                ):
                    # it is probably a condition
                    for arg in item.args:
                        if isinstance(arg, ast.Identifier):
                            param_name = arg.parts[-1]
                            break
                    if param_name is not None:
                        break

                if item.alias is not None:
                    # it is probably a query target
                    param_name = item.alias.parts[-1]
                    break

            return to_parameter(param_name, node.value)

        if isinstance(query, ast.Update):
            for name, value in dict(query.update_columns).items():
                if isinstance(value, ast.Constant):
                    query.update_columns[name] = to_parameter(name, value.value)
                else:
                    default_param_name = name
                    query_traversal(value, find_constants_f)

        elif isinstance(query, ast.Insert):
            # iterate over node.values and do some processing
            if query.values:
                values = []
                for row in query.values:
                    row2 = []
                    for i, val in enumerate(row):
                        if isinstance(val, ast.Constant):
                            param_name = None
                            if query.columns and i < len(query.columns):
                                param_name = query.columns[i].name
                            elif query.table:
                                param_name = query.table.parts[-1]
                            val = to_parameter(param_name, val.value)
                        row2.append(val)
                    values.append(row2)
                query.values = values

        default_param_name = "param"
        query_traversal(query, find_constants_f)

        # to lists:
        databases = {k: list(v) for k, v in databases.items()}
        response = {"query": str(query), "parameters": parameters, "databases": databases}
        return response, 200


@ns_conf.route("/list_databases")
@ns_conf.param("list_databases", "lists databases of mindsdb")
class ListDatabases(Resource):
    @ns_conf.doc("list_databases")
    @api_endpoint_metrics("GET", "/sql/list_databases")
    def get(self):
        listing_query = "SHOW DATABASES"
        mysql_proxy = FakeMysqlProxy()
        try:
            result: SQLAnswer = mysql_proxy.process_query(listing_query)

            # iterate over result.data and perform a query on each item to get the name of the tables
            if result.type == SQL_RESPONSE_TYPE.ERROR:
                listing_query_response = {
                    "type": "error",
                    "error_code": result.error_code,
                    "error_message": result.error_message,
                }
            elif result.type == SQL_RESPONSE_TYPE.OK:
                listing_query_response = {"type": "ok"}
            elif result.type == SQL_RESPONSE_TYPE.TABLE:
                listing_query_response = {
                    "data": [
                        {
                            "name": db_row[0],
                            "tables": [
                                table_row[0]
                                for table_row in mysql_proxy.process_query(
                                    "SHOW TABLES FROM `{}`".format(db_row[0])
                                ).result_set.to_lists()
                            ],
                        }
                        for db_row in result.result_set.to_lists()
                    ]
                }
        except Exception as e:
            logger.exception("Error while retrieving list of databases")
            listing_query_response = {
                "type": "error",
                "error_code": 0,
                "error_message": str(e),
            }

        return listing_query_response, 200


def _convert_result_to_copy_format(
    result: SQLAnswer, format_type: str = "csv"
) -> str:
    """
    Convert SQL query result to a copy-friendly format.

    Args:
        result: SQLAnswer object containing the query result
        format_type: Format type - 'csv', 'tsv', or 'tab' (default: 'csv')

    Returns:
        str: Formatted string ready to be copied to clipboard
    """
    if result.type not in (
        SQL_RESPONSE_TYPE.TABLE,
        SQL_RESPONSE_TYPE.COLUMNS_TABLE,
    ):
        msg = "Result must be a table type to convert to copy format"
        raise ValueError(msg)

    if result.result_set is None:
        return ""

    column_names = [
        column.alias or column.name or "" for column in result.result_set.columns
    ]

    data = result.result_set.to_lists(json_types=True)

    def _serialize_value(value):
        """Serialize a value for CSV/TSV output."""
        if value is None:
            return ""
        if isinstance(value, (list, dict)):
            try:
                return json.dumps(value, ensure_ascii=False)
            except (TypeError, ValueError):
                return str(value)
        if isinstance(value, bool):
            return str(value).lower()
        return str(value)

    if format_type.lower() in ("tsv", "tab"):
        def _escape_tsv_value(value):
            """Escape tabs and newlines in TSV values."""
            if isinstance(value, str):
                return (
                    value
                    .replace("\t", " ")
                    .replace("\n", " ")
                    .replace("\r", " ")
                )
            return str(value)

        serialized_data = [
            [_escape_tsv_value(_serialize_value(item)) for item in row]
            for row in data
        ]
        escaped_column_names = (
            [_escape_tsv_value(name) for name in column_names]
        )
        all_rows = [escaped_column_names] + serialized_data
        return "\n".join(["\t".join(row) for row in all_rows])
    else:
        output = StringIO()
        writer = csv.writer(output, dialect="excel")
        writer.writerow(column_names)
        for row in data:
            writer.writerow([_serialize_value(item) for item in row])
        return output.getvalue()


@ns_conf.route("/query/copy")
@ns_conf.param("query", "Execute query and return results in copy-friendly format")
class QueryCopy(Resource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @ns_conf.doc("query_copy")
    @api_endpoint_metrics("POST", "/sql/query/copy")
    @mark_process(name="http_query_copy")
    def post(self):
        """
        Execute a SQL query and return results in a copy-friendly format (CSV or TSV).
        This endpoint is designed to be used by the frontend to enable copy-to-clipboard functionality.
        
        Request body:
        - query: SQL query string
        - context: Optional context dictionary
        - params: Optional parameters dictionary
        - format: Optional format type - 'csv' (default) or 'tsv'/'tab'
        
        Returns:
        - text/plain response with formatted data ready for clipboard
        """
        query = request.json.get("query")
        context = request.json.get("context", {})
        params = request.json.get("params", {})
        format_type = request.json.get("format", "csv").lower()

        if not isinstance(query, str):
            return http_error(HTTPStatus.BAD_REQUEST, "Wrong arguments", 'Please provide "query" with the request.')
        
        if format_type not in ("csv", "tsv", "tab"):
            return http_error(HTTPStatus.BAD_REQUEST, "Invalid format", 'Format must be "csv", "tsv", or "tab".')
        
        logger.debug(f"Incoming copy query: {query}")

        try:
            mysql_proxy = FakeMysqlProxy()
            mysql_proxy.set_context(context)
            result: SQLAnswer = mysql_proxy.process_query(query, params=params)
            
            if result.type == SQL_RESPONSE_TYPE.ERROR:
                return http_error(
                    HTTPStatus.BAD_REQUEST,
                    f"Query Error {result.error_code or 0}",
                    result.error_message or "Unknown error"
                )
            
            if result.type not in (SQL_RESPONSE_TYPE.TABLE, SQL_RESPONSE_TYPE.COLUMNS_TABLE):
                return http_error(
                    HTTPStatus.BAD_REQUEST,
                    "Invalid result type",
                    "Query must return table data to copy. Non-SELECT queries cannot be copied."
                )
            
            # Convert to copy-friendly format
            copy_text = _convert_result_to_copy_format(result, format_type)
            
            # Return as plain text
            return Response(
                copy_text,
                mimetype="text/plain",
                headers={
                    "Content-Disposition": f"inline; filename=query_result.{format_type if format_type != 'tab' else 'tsv'}"
                }
            )
            
        except ExecutorException as e:
            logger.warning(f"Error query processing: {e}")
            return http_error(HTTPStatus.BAD_REQUEST, "Query Error", str(e))
        except QueryError as e:
            logger.warning(f"Query failed: {e}")
            return http_error(HTTPStatus.BAD_REQUEST, "Query Error", str(e))
        except UnknownError as e:
            logger.exception("Error query processing:")
            return http_error(HTTPStatus.INTERNAL_SERVER_ERROR, "Internal Error", str(e))
        except Exception as e:
            logger.exception("Error query processing:")
            return http_error(HTTPStatus.INTERNAL_SERVER_ERROR, "Internal Error", str(e))