import copy
import traceback
import datetime
from dateutil.parser import parse as parse_datetime
import subprocess
import os

from flask import request
from flask_restx import Resource, abort
from flask import current_app as ca

from mindsdb.utilities.log import log
from mindsdb.api.http.namespaces.configs.config import ns_conf
from mindsdb.utilities.functions import get_all_models_meta_data
from mindsdb.utilities.log import get_logs
from mindsdb.integrations import CHECKERS

def get_integration(name):
    integrations = ca.config_obj.get('integrations', {})
    return integrations.get(name)


@ns_conf.route('/logs')
@ns_conf.param('name', 'Get logs')
class GetLogs(Resource):
    @ns_conf.doc('get_integrations')
    def get(self):
        min_timestamp = parse_datetime(request.args['min_timestamp'])
        max_timestamp = request.args.get('max_timestamp', None)
        context = request.args.get('context', None)
        level = request.args.get('level', None)
        log_from = request.args.get('log_from', None)
        limit = request.args.get('limit', None)

        logs = get_logs(min_timestamp, max_timestamp, context, level, log_from, limit)
        return {'data': logs}

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
            db_type = params.get('type')
            checker_class = CHECKERS.get(db_type, None)
            if checker_class is None:
                abort(400, f"Unknown integration type: {db_type}")
            checker = checker_class(**params)
            return {'success': checker.check_connection()}, 200

        integration = get_integration(name)
        if integration is not None:
            abort(400, f"Integration with name '{name}' already exists")

        try:
            if 'enabled' in params:
                params['publish'] = params['enabled']
                del params['enabled']
            ca.config_obj.add_db_integration(name, params)

            model_data_arr = get_all_models_meta_data(ca.naitve_interface, ca.custom_models)
            ca.dbw.setup_integration(name)
            if is_test is False:
                ca.dbw.register_predictors(model_data_arr, name)
        except Exception as e:
            log.error(str(e))
            abort(500, f'Error during config update: {str(e)}')

        return '', 200

    @ns_conf.doc('delete_integration')
    def delete(self, name):
        integration = get_integration(name)
        if integration is None:
            abort(400, f"Nothing to delete. '{name}' not exists.")
        try:
            ca.config_obj.remove_db_integration(name)
        except Exception as e:
            log.error(str(e))
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
            log.error(str(e))
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

@ns_conf.route('/vars')
class Vars(Resource):
    def get(self):
        telemtry = True
        if os.getenv('CHECK_FOR_UPDATES', '1').lower() in ['0', 'false', 'False']:
            telemtry = False

        if ca.config_obj.get('disable_mongo', False):
            mongo = False
        else:
            mongo = True

        cloud = ca.config_obj.get('cloud', False)


        return {'mongo': mongo, 'telemtry': telemtry, 'cloud': cloud}

@ns_conf.param('flag', 'Turn telemtry on or off')
class ToggleTelemetry(Resource):
    @ns_conf.doc('check')
    def get(self, flag):
        if flag in ["True", "true", "t"]:
            return 'Enabled telemetry', 200
        else:
            return 'Disabled telemetry', 200


@ns_conf.route('/install_options')
@ns_conf.param('dependency_list', 'Install dependencies')
class InstallDependenciesList(Resource):
    def get(self):
        return {'dependencies':['snowflake','athena','google','s3','lightgbm_gpu','mssql']}

@ns_conf.route('/install/<dependency>')
@ns_conf.param('dependency', 'Install dependencies')
class InstallDependencies(Resource):
    def get(self, dependency):
        if dependency == 'snowflake':
            dependency = ['snowflake-connector-python[pandas]', 'asn1crypto==1.3.0']
        elif dependency == 'athena':
            dependency = ['PyAthena >= 2.0.0']
        elif dependency == 'google':
            dependency = ['google-cloud-storage', 'google-auth']
        elif dependency == 's3':
            dependency = ['boto3 >= 1.9.0']
        elif dependency == 'lightgbm_gpu':
            dependency = ['lightgbm', '--install-option=--gpu', '--upgrade']
        elif dependency == 'mssql':
            dependency = ['pymssql >= 2.1.4']
        else:
            return f'Unkown dependency: {dependency}', 400

        try:
            sp = subprocess.Popen(['pip3', 'install', *dependency])
            sp.wait()
        except Exception:
            try:
                sp = subprocess.Popen(['pip', 'install', *dependency])
                sp.wait()
            except Exception:
                return 'Failed to install', 400

        return 'Installed', 200
