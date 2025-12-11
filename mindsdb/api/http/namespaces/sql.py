import time
from http import HTTPStatus
from collections import defaultdict

from flask import request
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


from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser import ast
from mindsdb_sql_parser.ast import Constant, Identifier, Select
from mindsdb.integrations.utilities.query_traversal import query_traversal

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

@ns_conf.route("/query/constants")
@ns_conf.param("query", "Get Constants for the query")
class QueryConstants(Resource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def find_constants_with_identifiers(self, node, replace_constants=False, identifiers_to_replace={}):
        identifier_to_constant = {}
        identifier_count = {}
        aliases = set()
        last_identifier = None

        def process_constant(identifier_str, const_node):
            key_identifier_str = identifier_str
            if key_identifier_str in identifier_count:
                identifier_count[key_identifier_str] += 1
                key_identifier_str += str(identifier_count[key_identifier_str])
            else:
                identifier_count[key_identifier_str] = 0

            identifier_to_constant[key_identifier_str] = (
                identifier_str,
                const_node.value,
                type(const_node.value).__name__,
            )

            if replace_constants and key_identifier_str in identifiers_to_replace:
                const_node.value = "@" + identifiers_to_replace[key_identifier_str]

        def callback(n, **kwargs):
            nonlocal last_identifier

            if isinstance(n, Identifier):
                last_identifier = n
                if n.alias and n.alias.parts and n.alias.parts[0] not in aliases:
                    aliases.add(n.alias.parts[0])
            elif isinstance(n, Constant):
                if last_identifier:
                    identifier_str = last_identifier.get_string()
                    process_constant(identifier_str, n)

            return None

        if isinstance(node, ast.Update):
            for k, v in node.update_columns.items():
                if isinstance(v, Constant):
                    identifier_str = k
                    process_constant(identifier_str, v)
                elif isinstance(v, ast.Case):
                    # iterate over v.rules and do some processing
                    for rule in v.rules:
                        if isinstance(rule[1], ast.Constant):
                            identifier_str = k
                            process_constant(identifier_str, rule[1])
                        query_traversal(rule[0], callback)
                    # process default
                    if isinstance(v.default, ast.Constant):
                        identifier_str = k
                        process_constant(identifier_str, v.default)
                else:
                    query_traversal(v, callback)
            query_traversal(node.where, callback)
        elif isinstance(node, ast.Insert):
            # iterate over node.values and do some processing
            if node.values:
                for row in node.values:
                    for i, val in enumerate(row):
                        if isinstance(val, Constant):
                            if node.columns and i < len(node.columns):
                                identifier_str = node.columns[i].name
                                process_constant(identifier_str, val)
                            elif node.table:
                                identifier_str = node.table.get_string()
                                if len(node.table.parts) > 1:
                                    identifier_str = node.table.parts[1]
                                process_constant(identifier_str, val)
            elif node.from_select:
                query_traversal(node.from_select, callback)
        else:
            query_traversal(node, callback)

        return (identifier_to_constant, aliases)

    def get_children(self, node):
        if hasattr(node, "children"):
            return node.children
        elif isinstance(node, Select):
            children = []

            if node.from_table and node.cte is None:
                children.append(node.from_table)
            if node.cte:
                for cte in node.cte:
                    children.append(cte.query)
            if node.where:
                children.append(node.where)

            return children
        elif isinstance(node, ast.Join):
            children = []
            if node.left:
                children.append(node.left)
            if node.right:
                children.append(node.right)
            return children
        elif isinstance(node, ast.Update):
            children = []
            if node.table:
                children.append(node.table)
            if node.update_columns:
                for k, v in node.update_columns.items():
                    children.append(v)
            return children
        elif isinstance(node, ast.Insert):
            children = []
            if node.table:
                children.append(node.table)
            if node.from_select:
                children.append(node.from_select)
            return children
        elif isinstance(node, ast.Operation):
            children = []
            if len(node.args) >= 1 and node.args[0]:
                children.append(node.args[0])
            if len(node.args) == 2 and node.args[1]:
                children.append(node.args[1])
            return children
        elif isinstance(node, ast.Union):
            children = []
            if node.left:
                children.append(node.left)
            if node.right:
                children.append(node.right)
            return children
        else:
            return []

    def extract_datasource_and_tables(self, node):
        """Extract datasource and tables information from an AST node.

        Args:
            node: The AST node to analyze

        Returns:
            dict: A dictionary containing:
                - datasources_with_tables: Dictionary mapping datasources to their tables
                - tables: List of all table names
                - datsources: List of all datasource names
        """
        datasources_with_tables = {}
        tables = []
        datsources = []

        def handle_identifier(identifier):
            parts = identifier.parts
            if len(parts) >= 2:
                # Has both datasource and table
                datasource = parts[0]
                table = parts[1]

                # Add to datasources_with_tables mapping
                if datasource not in datasources_with_tables:
                    datasources_with_tables[datasource] = []
                if table not in datasources_with_tables[datasource]:
                    datasources_with_tables[datasource].append(table)

                # Add to overall lists
                datsources.append(datasource)
                tables.append(table)

        def callback(node, **kwargs):
            # Handle table references in FROM clauses
            if isinstance(node, ast.Select):
                if node.from_table:
                    if isinstance(node.from_table, Identifier):
                        handle_identifier(node.from_table)

            # Handle INSERT statements
            elif isinstance(node, ast.Insert):
                if isinstance(node.table, Identifier):
                    handle_identifier(node.table)

            # Handle UPDATE statements
            elif isinstance(node, ast.Update):
                if isinstance(node.table, Identifier):
                    handle_identifier(node.table)

            # Handle JOIN clauses
            elif isinstance(node, ast.Join):
                if isinstance(node.right, Identifier):
                    handle_identifier(node.right)
                if isinstance(node.left, Identifier):
                    handle_identifier(node.left)

        query_traversal(node, callback)

        datasource = ""
        if len(datasources_with_tables) > 0:
            datasource = list(datasources_with_tables.keys())[0]

        return {
            "datasources_with_tables": datasources_with_tables,
            "datasource": datasource,
        }

    @ns_conf.doc("query_constants")
    @api_endpoint_metrics("POST", "/sql/query/constants")
    def post(self):
        query = request.json["query"]
        replace_constants = request.json.get("replace_constants", False)
        identifiers_to_replace = request.json.get("identifiers_to_replace", {})

        try:
            query_ast = parse_sql(query)
            parameterized_query = query
            datasource = ""
            datasources_with_tables = {}
            (constants_with_identifiers, aliases) = self.find_constants_with_identifiers(
                query_ast,
                replace_constants=replace_constants,
                identifiers_to_replace=identifiers_to_replace,
            )
            if replace_constants:
                parameterized_query = query_ast.to_string()
            else:
                datasource_response = self.extract_datasource_and_tables(query_ast)
                datasource = datasource_response["datasource"]
                datasources_with_tables = datasource_response["datasources_with_tables"]

            response = {
                "constant_with_identifiers": constants_with_identifiers,
                "parameterized_query": parameterized_query,
                "datasource": datasource,
                "datasources_with_tables": datasources_with_tables,
            }
            query_response = {"type": SQL_RESPONSE_TYPE.OK, "data": response}
        except Exception as e:
            query_response = {
                "type": SQL_RESPONSE_TYPE.ERROR,
                "error_code": 0,
                "error_message": str(e),
            }

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
