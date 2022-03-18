from flask_restx import Resource
from flask import request

from mindsdb.api.http.namespaces.configs.sql import ns_conf
from mindsdb.api.mysql.mysql_proxy.mysql_proxy import FakeMysqlProxy, ANSWER_TYPE as SQL_ANSWER_TYPE


@ns_conf.route('/query')
@ns_conf.param('query', 'Execute query')
class Query(Resource):
    @ns_conf.doc('query')
    def post(self):
        query = request.json['query']

        mysql_proxy = FakeMysqlProxy(company_id=request.company_id)
        try:
            result = mysql_proxy.process_query(query)
            if result.type == SQL_ANSWER_TYPE.ERROR:
                query_response = {
                    'type': 'error',
                    'error_code': result.error_code,
                    'error_message': result.error_message
                }
            elif result.type == SQL_ANSWER_TYPE.OK:
                query_response = {
                    'type': 'ok'
                }
            elif result.type == SQL_ANSWER_TYPE.TABLE:
                query_response = {
                    'type': 'table',
                    'data': result.data,
                    'column_names': [x['alias'] or x['name'] if 'alias' in x else x['name'] for x in result.columns]
                }
        except Exception as e:
            query_response = {
                'type': 'error',
                'error_code': 0,
                'error_message': str(e)
            }

        return query_response, 200
