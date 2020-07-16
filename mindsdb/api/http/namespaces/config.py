from flask import request
from flask_restx import Resource
from flask import current_app as ca

from mindsdb.api.http.namespaces.configs.config import ns_conf


@ns_conf.route('/integrations/<name>')
@ns_conf.param('name', 'Database integration')
class Integration(Resource):
    @ns_conf.doc('get_integration')
    def get(self, name):
        '''return datasource metadata'''
        return ca.config_obj['integrations'][name]

    @ns_conf.doc('put_integration')
    def put(self, name):
        '''return datasource metadata'''
        params = request.json.get('params')
        ca.config_obj.add_db_integration(name, params)
        return 'added'

    @ns_conf.doc('delete_integration')
    def delete(self, name):
        ca.config_obj.remove_db_integration(name)
        return 'deleted'

@ns_conf.route('/integrations/<name>/modify')
@ns_conf.param('name', 'Modify database integration')
class Integration(Resource):
    @ns_conf.doc('modify_integration')
    def put(self, name):
        '''return datasource metadata'''
        params = request.json.get('params')
        ca.config_obj.modify_db_integration(name, params)
        return 'modified'
