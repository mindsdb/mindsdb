from http import HTTPStatus
import traceback

from flask import request
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

logger = log.getLogger(__name__)


@ns_conf.route("/query")
@ns_conf.param("query", "Execute query")
class Query(Resource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @ns_conf.doc("query")
    @api_endpoint_metrics('POST', '/sql/query')
    def post(self):
        query = request.json["query"]
        context = request.json.get("context", {})

        if isinstance(query, str) is False or isinstance(context, dict) is False:
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Wrong arguments',
                'Please provide "query" with the request.'
            )
        logger.debug(f'Incoming query: {query}')

        if context.get("profiling") is True:
            profiler.enable()

        error_type = None
        error_code = None
        error_text = None
        error_traceback = None

        profiler.set_meta(
            query=query, api="http", environment=Config().get("environment")
        )
        with profiler.Context("http_query_processing"):
            mysql_proxy = FakeMysqlProxy()
            mysql_proxy.set_context(context)
            try:
                result: SQLAnswer = mysql_proxy.process_query(query)
                query_response: dict = result.dump_http_response()
            except ExecutorException as e:
                # classified error
                error_type = "expected"
                query_response = {
                    "type": SQL_RESPONSE_TYPE.ERROR,
                    "error_code": 0,
                    "error_message": str(e),
                }
                logger.error(f"Error query processing: \n{traceback.format_exc()}")

            except UnknownError as e:
                # unclassified
                error_type = "unexpected"
                query_response = {
                    "type": SQL_RESPONSE_TYPE.ERROR,
                    "error_code": 0,
                    "error_message": str(e),
                }
                logger.error(f"Error query processing: \n{traceback.format_exc()}")

            except Exception as e:
                error_type = "unexpected"
                query_response = {
                    "type": SQL_RESPONSE_TYPE.ERROR,
                    "error_code": 0,
                    "error_message": str(e),
                }
                logger.error(f"Error query processing: \n{traceback.format_exc()}")

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

        return query_response, 200


@ns_conf.route("/list_databases")
@ns_conf.param("list_databases", "lists databases of mindsdb")
class ListDatabases(Resource):
    @ns_conf.doc("list_databases")
    @api_endpoint_metrics('GET', '/sql/list_databases')
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
                    "data": [{
                        "name": db_row[0],
                        "tables": [
                            table_row[0]
                            for table_row in mysql_proxy.process_query(
                                "SHOW TABLES FROM `{}`".format(db_row[0])
                            ).result_set.to_lists()
                        ]
                    } for db_row in result.result_set.to_lists()]
                }
        except Exception as e:
            listing_query_response = {
                "type": "error",
                "error_code": 0,
                "error_message": str(e),
            }

        return listing_query_response, 200
