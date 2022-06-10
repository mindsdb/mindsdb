import time

from flask import request
from flask_restx import Resource
from pandas.core.frame import DataFrame

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import Constant, Identifier
from mindsdb_sql.planner.utils import query_traversal

from mindsdb.api.http.utils import http_error
from mindsdb.api.http.namespaces.configs.analysis import ns_conf
from mindsdb.api.mysql.mysql_proxy.classes.fake_mysql_proxy import FakeMysqlProxy
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE as SQL_RESPONSE_TYPE


@ns_conf.route('/query')
class QueryAnalysis(Resource):
    @ns_conf.doc('post_query_to_analyze')
    def post(self):
        data = request.json
        query = data.get('query')
        context = data.get('context', {})
        limit = data.get('limit')
        if query is None or len(query) == 0:
            return http_error(400, 'Missed query', 'Need provide query to analyze')

        try:
            ast = parse_sql(query)
        except Exception as e:
            return http_error(500, 'Wrong query', str(e))

        if limit is not None:
            ast.limit = Constant(limit)
            query = str(ast)

        mysql_proxy = FakeMysqlProxy(
            company_id=request.company_id,
            user_class=request.user_class
        )
        mysql_proxy.set_context(context)

        try:
            result = mysql_proxy.process_query(query)
        except Exception as e:
            import traceback
            print(traceback.format_exc())
            return http_error(500, 'Error', str(e))

        if result.type == SQL_RESPONSE_TYPE.ERROR:
            return http_error(500, f'Error {result.error_code}', result.error_message)
        if result.type != SQL_RESPONSE_TYPE.TABLE:
            return http_error(500, 'Error', 'Query does not return data')

        column_names = [x['name'] for x in result.columns]
        analysis = request.model_interface.analyse_dataset(
            df=DataFrame(result.data, columns=column_names),
            company_id=None
        )

        query_tables = []

        def find_tables(node, is_table, **kwargs):
            if is_table and isinstance(node, Identifier):
                query_tables.append('.'.join(node.parts))
        query_traversal(ast, find_tables)

        return {
            'analysis': analysis,
            'column_names': column_names,
            'row_count': len(result.data),
            'timestamp': time.time(),
            'tables': query_tables
        }
