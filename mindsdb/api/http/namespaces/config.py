import copy
import traceback

from flask import request
from flask_restx import Resource, abort
from flask import current_app as ca

from mindsdb.api.http.namespaces.configs.config import ns_conf
from mindsdb.utilities.functions import get_all_models_meta_data


def get_integration(name):
    integrations = ca.config_obj.get('integrations', {})
    return integrations.get(name)


@ns_conf.route('/integrations')
@ns_conf.param('name', 'List all database integration')
class ListIntegration(Resource):
    @ns_conf.doc('get_integrations')
    def get(self):
        return {'integrations': [k for k in ca.config_obj.get('integrations', {})]}


@ns_conf.route('/all_integrations')
@ns_conf.param('name', 'List all database integration')
class AllIntegration(Resource):
    @ns_conf.doc('get_all_integrations')
    def get(self):
        integrations = copy.deepcopy(
            ca.config_obj.get('integrations', {})
        )
        for integration in integrations.values():
            if 'password' in integration:
                integration['password'] = None
        return integrations


@ns_conf.route('/integrations/<name>')
@ns_conf.param('name', 'Database integration')
class Integration(Resource):
    @ns_conf.doc('get_integration')
    def get(self, name):
        integration = get_integration(name)
        if integration is None:
            abort(404, f'Can\'t find database integration: {name}')
        integration = copy.deepcopy(integration)
        if 'password' in integration:
            integration['password'] = None
        return integration

    @ns_conf.doc('put_integration')
    def put(self, name):
        params = request.json.get('params')
        if not isinstance(params, dict):
            abort(400, "type of 'params' must be dict")

        is_test = params.get('test', False)
        if is_test:
            del params['test']

        integration = get_integration(name)
        if integration is not None:
            abort(400, f"Integration with name '{name}' already exists")
        try:
            if 'enabled' in params:
                params['publish'] = params['enabled']
                del params['enabled']
            ca.config_obj.add_db_integration(name, params)

            mdb = ca.mindsdb_native
            cst = ca.custom_models
            model_data_arr = get_all_models_meta_data(mdb, cst)
            ca.dbw.setup_integration(name)
            if is_test is False:
                ca.dbw.register_predictors(model_data_arr)
        except Exception as e:
            print(traceback.format_exc())
            abort(500, f'Error during config update: {str(e)}')

        if is_test:
            cons = ca.dbw.check_connections()
            ca.config_obj.remove_db_integration(name)
            return {'success': cons[name]}, 200

        return '', 200

    @ns_conf.doc('delete_integration')
    def delete(self, name):
        integration = get_integration(name)
        if integration is None:
            abort(400, f"Nothing to delete. '{name}' not exists.")
        try:
            ca.config_obj.remove_db_integration(name)
        except Exception as e:
            print(traceback.format_exc())
            abort(500, f'Error during integration delete: {str(e)}')
        return '', 200

    @ns_conf.doc('modify_integration')
    def post(self, name):
        params = request.json.get('params')
        if not isinstance(params, dict):
            abort(400, "type of 'params' must be dict")
        integration = get_integration(name)
        if integration is None:
            abort(400, f"Nothin to modify. '{name}' not exists.")
        try:
            if 'enabled' in params:
                params['publish'] = params['enabled']
                del params['enabled']
            ca.config_obj.modify_db_integration(name, params)
            ca.dbw.setup_integration(name)
        except Exception as e:
            print(traceback.format_exc())
            abort(500, f'Error during integration modifycation: {str(e)}')
        return '', 200


@ns_conf.route('/integrations/<name>/check')
@ns_conf.param('name', 'Database integration checks')
class Check(Resource):
    @ns_conf.doc('check')
    def get(self, name):
        if get_integration(name) is None:
            abort(404, f'Can\'t find database integration: {name}')
        connections = ca.dbw.check_connections()
        return connections.get(name, False), 200


@ns_conf.route('/telemetry/<flag>')
@ns_conf.param('flag', 'Turn telemtry on or off')
class ToggleTelemetry(Resource):
    @ns_conf.doc('check')
    def get(self, flag):
        if flag in ["True", "true", "t"]:
            return 'Enabled telemetry', 200
        else:
            return 'Disabled telemetry', 200
