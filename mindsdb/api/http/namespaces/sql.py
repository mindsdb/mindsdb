from flask_restx import Resource
from flask import request

from mindsdb.api.http.namespaces.configs.sql import ns_conf
from mindsdb.api.mysql.mysql_proxy.mysql_proxy import FakeMysqlProxy, RESPONSE_TYPE as SQL_RESPONSE_TYPE


@ns_conf.route('/query')
@ns_conf.param('query', 'Execute query')
class Query(Resource):
    @ns_conf.doc('query')
    def post(self):
        query = request.json['query']
        context = request.json.get('context', {})

        mysql_proxy = FakeMysqlProxy(company_id=request.company_id)
        mysql_proxy.set_context(context)
        try:
            result = mysql_proxy.process_query(query)
            if result.type == SQL_RESPONSE_TYPE.ERROR:
                listing_query_response = {
                    'type': 'error',
                    'error_code': result.error_code,
                    'error_message': result.error_message
                }
            elif result.type == SQL_RESPONSE_TYPE.OK:
                listing_query_response = {
                    'type': 'ok'
                }
            elif result.type == SQL_RESPONSE_TYPE.TABLE:
                listing_query_response = {
                    'type': 'table',
                    'data': result.data,
                    'column_names': [x['alias'] or x['name'] if 'alias' in x else x['name'] for x in result.columns]
                }
        except Exception as e:
            listing_query_response = {
                'type': 'error',
                'error_code': 0,
                'error_message': str(e)
            }

        context = mysql_proxy.get_context(context)

        listing_query_response['context'] = context

        return listing_query_response, 200


@ns_conf.route('/list_databases')
@ns_conf.param('list_databases', 'lists databases of mindsdb')
class ListDatabases(Resource):
    @ns_conf.doc('list_databases')
    def get(self):
        listing_query = 'SHOW DATABASES'
        mysql_proxy = FakeMysqlProxy(company_id=request.company_id)
        try:
            result = mysql_proxy.process_query(listing_query)

            # iterate over result.data and perform a query on each item to get the name of the tables
            if result.type == SQL_RESPONSE_TYPE.ERROR:
                listing_query_response = {
                    'type': 'error',
                    'error_code': result.error_code,
                    'error_message': result.error_message
                }
            elif result.type == SQL_RESPONSE_TYPE.OK:
                listing_query_response = {
                    'type': 'ok'
                }
            elif result.type == SQL_RESPONSE_TYPE.TABLE:
                listing_query_response = {
                    'data': [{'name': x[0], 'tables': mysql_proxy.process_query('SHOW TABLES FROM `{}`'.format(x[0])).data} for x in result.data]
                }
        except Exception as e:
            listing_query_response = {
                'type': 'error',
                'error_code': 0,
                'error_message': str(e)
            }

        return listing_query_response, 200
