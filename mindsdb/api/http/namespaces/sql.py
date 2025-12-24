import time
from http import HTTPStatus
from enum import Enum

from flask import request, Response
from flask_restx import Resource

import mindsdb.utilities.hooks as hooks
import mindsdb.utilities.profiler as profiler
from mindsdb.api.http.utils import http_error
from mindsdb.api.http.namespaces.configs.sql import ns_conf
from mindsdb.api.mysql.mysql_proxy.mysql_proxy import SQLAnswer
from mindsdb.api.mysql.mysql_proxy.classes.fake_mysql_proxy import FakeMysqlProxy
from mindsdb.api.executor.data_types.response_type import (
    RESPONSE_TYPE as SQL_RESPONSE_TYPE,
)
from mindsdb.api.executor.exceptions import ExecutorException, UnknownError
from mindsdb.metrics.metrics import api_endpoint_metrics
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.exception import QueryError
from mindsdb.utilities.functions import mark_process

logger = log.getLogger(__name__)


class ReponseFormat(Enum):
    DEFAULT = None
    SSE = "sse"
    JSONLINES = "jsonlines"


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
        try:
            response_format = ReponseFormat(request.json.get('response_format', None))
        except ValueError:
            return http_error(HTTPStatus.BAD_REQUEST, "Invalid stream format", 'Please provide a valid stream format.')

        if isinstance(query, str) is False or isinstance(context, dict) is False:
            return http_error(HTTPStatus.BAD_REQUEST, "Wrong arguments", 'Please provide "query" with the request.')
        logger.debug(f"Incoming query: {query}")

        if context.get("profiling") is True:
            profiler.enable()

        error_type = None
        error_traceback = None

        profiler.set_meta(query=query, api="http", environment=Config().get("environment"))
        with profiler.Context("http_query_processing"):
            mysql_proxy = FakeMysqlProxy()
            mysql_proxy.set_context(context)
            try:
                result: SQLAnswer = mysql_proxy.process_query(query, params=params)
            except ExecutorException as e:
                # classified error
                error_type = "expected"
                result = SQLAnswer(
                    resp_type=SQL_RESPONSE_TYPE.ERROR,
                    error_code=0,
                    error_message=str(e)
                )
                logger.warning(f"Error query processing: {e}")
            except QueryError as e:
                error_type = "expected" if e.is_expected else "unexpected"
                result = SQLAnswer(
                    resp_type=SQL_RESPONSE_TYPE.ERROR,
                    error_code=0,
                    error_message=str(e)
                )
                if e.is_expected:
                    logger.warning(f"Query failed due to expected reason: {e}")
                else:
                    logger.exception("Error query processing:")
            except (UnknownError, Exception) as e:
                error_type = "unexpected"
                result = SQLAnswer(
                    resp_type=SQL_RESPONSE_TYPE.ERROR,
                    error_code=0,
                    error_message=str(e)
                )
                logger.exception("Error query processing:")

            context = mysql_proxy.get_context()

            if response_format == ReponseFormat.JSONLINES:
                query_response = result.stream_http_response(context=context)
                query_response = Response(query_response, mimetype='application/jsonlines')
            elif response_format == ReponseFormat.SSE:
                query_response = result.stream_http_response(context=context)
                query_response = Response(query_response, mimetype='text/event-stream')
            else:
                query_response = result.dump_http_response(context=context), 200

        hooks.after_api_query(
            company_id=ctx.company_id,
            api="http",
            command=None,
            payload=query,
            error_type=error_type,
            error_code=result.error_code,
            error_text=result.error_message,
            traceback=error_traceback,
        )

        end_time = time.time()
        log_msg = f"SQL processed in {(end_time - start_time):.2f}s ({end_time:.2f}-{start_time:.2f}), result is {result.type}, "
        if result.type is SQL_RESPONSE_TYPE.TABLE and response_format is ReponseFormat.DEFAULT:
            log_msg += f" one-piece result ({len(query_response[0]['data'])} rows), "
        elif result.type is SQL_RESPONSE_TYPE.TABLE:
            log_msg += f" {response_format} result, "
        elif result.type is SQL_RESPONSE_TYPE.ERROR:
            log_msg += f" ({result.error_message}), "
        log_msg += f"used handlers: {ctx.used_handlers}"
        logger.debug(log_msg)

        return query_response


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
