import time

import pandas as pd
from flask import request
from flask_restx import Resource
from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser.ast import Constant
from pandas.core.frame import DataFrame

from mindsdb.api.http.namespaces.configs.analysis import ns_conf
from mindsdb.api.executor.utilities.sql import get_query_tables
from mindsdb.api.http.utils import http_error
from mindsdb.api.mysql.mysql_proxy.classes.fake_mysql_proxy import FakeMysqlProxy
from mindsdb.api.executor.data_types.response_type import (
    RESPONSE_TYPE as SQL_RESPONSE_TYPE,
)
from mindsdb.metrics.metrics import api_endpoint_metrics
from mindsdb.utilities import log

logger = log.getLogger(__name__)


def analyze_df(df: DataFrame) -> dict:
    if len(df) == 0:
        return {}

    cols = pd.Series(df.columns)

    # https://stackoverflow.com/questions/24685012/pandas-dataframe-renaming-multiple-identically-named-columns
    for dup in cols[cols.duplicated()].unique():
        cols[cols[cols == dup].index.values.tolist()] = [
            dup + "." + str(i) if i != 0 else dup for i in range(sum(cols == dup))
        ]

    # rename the columns with the cols list.
    df.columns = cols

    from dataprep_ml.insights import analyze_dataset
    analysis = analyze_dataset(df)
    return analysis.to_dict()


@ns_conf.route("/query")
class QueryAnalysis(Resource):
    @ns_conf.doc("post_query_to_analyze")
    @api_endpoint_metrics('POST', '/analysis/query')
    def post(self):
        data = request.json
        query = data.get("query")
        context = data.get("context", {})
        limit = data.get("limit")
        if query is None or len(query) == 0:
            return http_error(400, "Missed query", "Need provide query to analyze")

        try:
            ast = parse_sql(query)
        except Exception as e:
            return http_error(500, "Wrong query", str(e))

        if limit is not None:
            ast.limit = Constant(limit)
            query = str(ast)

        mysql_proxy = FakeMysqlProxy()
        mysql_proxy.set_context(context)

        try:
            result = mysql_proxy.process_query(query)
        except Exception as e:
            import traceback

            logger.error(traceback.format_exc())
            return http_error(500, "Error", str(e))

        if result.type == SQL_RESPONSE_TYPE.ERROR:
            return http_error(500, f"Error {result.error_code}", result.error_message)
        if result.type != SQL_RESPONSE_TYPE.TABLE:
            return http_error(500, "Error", "Query does not return data")

        column_names = [column.name for column in result.result_set.columns]
        df = result.result_set.to_df()
        try:
            analysis = analyze_df(df)
        except ImportError:
            return {
                'analysis': {},
                'timestamp': time.time(),
                'error': 'To use this feature, please install the "dataprep_ml" package.'
            }

        query_tables = [
            table.to_string() for table in get_query_tables(ast)
        ]

        return {
            "analysis": analysis,
            "column_names": column_names,
            "row_count": len(result.result_set),
            "timestamp": time.time(),
            "tables": query_tables,
        }


@ns_conf.route("/data")
class DataAnalysis(Resource):
    @ns_conf.doc("post_data_to_analyze")
    @api_endpoint_metrics('POST', '/analysis/data')
    def post(self):
        payload = request.json
        column_names = payload.get("column_names")
        data = payload.get("data")

        timestamp = time.time()
        try:
            analysis = analyze_df(DataFrame(data, columns=column_names))
            return {"analysis": analysis, "timestamp": time.time()}
        except ImportError:
            return {
                'analysis': {},
                'timestamp': timestamp,
                'error': 'To use this feature, please install the "dataprep_ml" package.'
            }
        except Exception as e:
            # Don't want analysis exceptions to show up on UI.
            # TODO: Fix analysis so it doesn't throw exceptions at all.
            return {
                'analysis': {},
                'timestamp': timestamp,
                'error': str(e)
            }
