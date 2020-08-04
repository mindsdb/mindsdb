from flask import request
from flask_restx import Resource
from flask import current_app as ca

from mindsdb.api.http.namespaces.configs.config import ns_conf
from mindsdb.interfaces.database.database import DatabaseWrapper


@ns_conf.route('/integrations')
@ns_conf.param('name', 'List all database integration')
class Integration(Resource):
    @ns_conf.doc('get_integrations')
    def get(self):
        return {'integrations': [k for k in ca.config_obj['integrations']]}

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
        DatabaseWrapper(ca.config_obj)
        return 'added'

    @ns_conf.doc('delete_integration')
    def delete(self, name):
        ca.config_obj.remove_db_integration(name)
        return 'deleted'

    @ns_conf.doc('modify_integration')
    def post(self, name):
        '''return datasource metadata'''
        params = request.json.get('params')
        ca.config_obj.modify_db_integration(name, params)
        DatabaseWrapper(ca.config_obj)
        return 'modified'


@ns_conf.route('/integrations/<name>/check')
@ns_conf.param('name', 'Database integration checks')
class Check(Resource):
    @ns_conf.doc('check')
    def get(self, name):
        '''return datasource metadata'''
        dbw = DatabaseWrapper(config_obj)
        for db_name, connected in dbw.check_connections().items():
            if db_name == name:
                returne 200, connected
        return 400, f'Can\'t find database integration: {name}'
