import copy
import datetime
from dateutil.parser import parse as parse_datetime
import os
import tempfile
import shutil
from pathlib import Path

from flask import request
from flask_restx import Resource, abort
from flask import current_app as ca
from dateutil.tz import tzlocal

from mindsdb.utilities import log
from mindsdb.api.http.namespaces.configs.config import ns_conf
from mindsdb.utilities.log_controller import get_logs
from mindsdb.interfaces.stream.stream import StreamController


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
    def get(self):
        return {
            'integrations': [k for k in ca.integration_controller.get_all(sensitive_info=False)]
        }


@ns_conf.route('/all_integrations')
@ns_conf.param('name', 'List all database integration')
class AllIntegration(Resource):
    @ns_conf.doc('get_all_integrations')
    def get(self):
        integrations = ca.integration_controller.get_all(sensitive_info=False)
        return integrations


@ns_conf.route('/integrations/<name>')
@ns_conf.param('name', 'Database integration')
class Integration(Resource):
    @ns_conf.doc('get_integration')
    def get(self, name):
        integration = ca.integration_controller.get(name, sensitive_info=False)
        if integration is None:
            abort(404, f'Can\'t find database integration: {name}')
        integration = copy.deepcopy(integration)
        return integration

    @ns_conf.doc('put_integration')
    def put(self, name):
        params = {}
        params.update((request.json or {}).get('params', {}))
        params.update(request.form or {})

        if len(params) == 0:
            abort(400, "type of 'params' must be dict")

        files = request.files
        temp_dir = None
        if files is not None and len(files) > 0:
            temp_dir = tempfile.mkdtemp(prefix='integration_files_')
            for key, file in files.items():
                temp_dir_path = Path(temp_dir)
                file_name = Path(file.filename)
                file_path = temp_dir_path.joinpath(file_name).resolve()
                if temp_dir_path not in file_path.parents:
                    raise Exception(f'Can not save file at path: {file_path}')
                file.save(file_path)
                params[key] = file_path

        is_test = params.get('test', False)
        if is_test:
            del params['test']

            handler = ca.integration_controller.create_tmp_handler(
                handler_type=params.get('type'),
                connection_data=params
            )
            status = handler.check_connection()
            if temp_dir is not None:
                shutil.rmtree(temp_dir)
            return status, 200

        integration = ca.integration_controller.get(name, sensitive_info=False)
        if integration is not None:
            abort(400, f"Integration with name '{name}' already exists")

        try:
            engine = params['type']
            if engine is not None:
                del params['type']
            ca.integration_controller.add(name, engine, params)

            if is_test is False and params.get('publish', False) is True:
                stream_controller = StreamController()
                if engine in stream_controller.known_dbs and params.get('publish', False) is True:
                    stream_controller.setup(name)
        except Exception as e:
            log.logger.error(str(e))
            if temp_dir is not None:
                shutil.rmtree(temp_dir)
            abort(500, f'Error during config update: {str(e)}')

        if temp_dir is not None:
            shutil.rmtree(temp_dir)
        return '', 200

    @ns_conf.doc('delete_integration')
    def delete(self, name):
        integration = ca.integration_controller.get(name)
        if integration is None:
            abort(400, f"Nothing to delete. '{name}' not exists.")
        try:
            ca.integration_controller.delete(name)
        except Exception as e:
            log.logger.error(str(e))
            abort(500, f'Error during integration delete: {str(e)}')
        return '', 200

    @ns_conf.doc('modify_integration')
    def post(self, name):
        params = {}
        params.update((request.json or {}).get('params', {}))
        params.update(request.form or {})

        if not isinstance(params, dict):
            abort(400, "type of 'params' must be dict")
        integration = ca.integration_controller.get(name)
        if integration is None:
            abort(400, f"Nothin to modify. '{name}' not exists.")
        try:
            if 'enabled' in params:
                params['publish'] = params['enabled']
                del params['enabled']
            ca.integration_controller.modify(name, params)

            stream_controller = StreamController()
            if params.get('type') in stream_controller.known_dbs and params.get('publish', False) is True:
                stream_controller.setup(name)
        except Exception as e:
            log.logger.error(str(e))
            abort(500, f'Error during integration modifycation: {str(e)}')
        return '', 200


@ns_conf.route('/integrations/<name>/check')
@ns_conf.param('name', 'Database integration checks')
class Check(Resource):
    @ns_conf.doc('check')
    def get(self, name):
        if ca.integration_controller.get(name) is None:
            abort(404, f'Can\'t find database integration: {name}')
        connections = ca.integration_controller.check_connections()
        return connections.get(name, False), 200


@ns_conf.route('/vars')
class Vars(Resource):
    def get(self):
        if os.getenv('CHECK_FOR_UPDATES', '1').lower() in ['0', 'false']:
            telemtry = False
        else:
            telemtry = True

        if ca.config_obj.get('disable_mongo', False):
            mongo = False
        else:
            mongo = True

        cloud = ca.config_obj.get('cloud', False)
        local_time = datetime.datetime.now(tzlocal())
        local_timezone = local_time.tzname()

        return {
            'mongo': mongo,
            'telemtry': telemtry,
            'cloud': cloud,
            'timezone': local_timezone,
        }
