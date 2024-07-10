import os
import json
import time
import datetime
import platform
import importlib
from pathlib import Path
import tempfile
import multipart

from flask import request, send_file, abort, current_app as ca
from flask_restx import Resource

from mindsdb.utilities import log

from mindsdb_sql.parser.ast import Identifier
from mindsdb_sql.parser.dialects.mindsdb import CreateMLEngine

from mindsdb.metrics.metrics import api_endpoint_metrics
from mindsdb.integrations.utilities.install import install_dependencies
from mindsdb.interfaces.storage.model_fs import HandlerStorage
from mindsdb.api.http.utils import http_error
from mindsdb.api.http.namespaces.configs.handlers import ns_conf
from mindsdb.api.executor.controllers.session_controller import SessionController
from mindsdb.api.executor.command_executor import ExecuteCommands
from mindsdb.utilities.config import Config

logger = log.getLogger(__name__)


@ns_conf.route('/')
class HandlersList(Resource):
    @ns_conf.doc('handlers_list')
    @api_endpoint_metrics('GET', '/handlers')
    def get(self):
        '''List all db handlers'''
        try:
            # TODO: is 10 the right number?
            top_10_data_sources, top_10_ai_engines = self._get_top_10_data_sources_and_ai_engines()
        except Exception as e:
            logger.error(f'Failed to get top 10 data sources and AI engines: {e}')
            top_10_data_sources, top_10_ai_engines = [], []

        handlers = ca.integration_controller.get_handlers_import_status()
        result = []
        for handler_type, handler_meta in handlers.items():
            # remove non-integration handlers
            if handler_type not in ['utilities', 'dummy_data']:
                row = {'name': handler_type}
                row.update(handler_meta)

                # check if handler is AWS product
                try:
                    row['is_aws'] = self._is_aws_product(handler_meta['title'])
                except Exception:
                    logger.error(f'Failed to check if {handler_type} is AWS product')
                    row['is_aws'] = False

                # check if handler is new using creation date
                try:
                    # TODO: is 3 months the right number?
                    # get the timestamp for three months ago
                    three_months_ago_timestamp = time.mktime((datetime.datetime.now() - datetime.timedelta(days=90)).timetuple())
                    # TODO: is __about__.py the right file to check?
                    # check if the handler was created before one month ago
                    row['is_new'] = self._get_creation_date(f'mindsdb/integrations/handlers/{handler_meta["import"]["folder"]}/__about__.py') > three_months_ago_timestamp
                except Exception:
                    logger.error(f'Failed to check if {handler_type} is new')
                    row['is_new'] = False

                # check if handler is in top 10 data sources or AI engines
                if handler_type in top_10_data_sources or handler_type in top_10_ai_engines:
                    row['most_popular'] = True
                else:
                    row['most_popular'] = False

                result.append(row)
        return result

    def _is_aws_product(self, handler_title):
        # TODO: can this list be more comprehensive?
        keywords = ['aws', 'amazon']

        for keyword in keywords:
            if keyword in handler_title.lower():
                return True

        return False

    def _get_creation_date(self, path_to_file):
        if platform.system() == 'Windows':
            return os.path.getctime(path_to_file)
        else:
            stat = os.stat(path_to_file)
            try:
                return stat.st_birthtime
            except AttributeError:
                return stat.st_mtime

    def _get_top_10_data_sources_and_ai_engines(self):
        from mindsdb.integrations.handlers.bigquery_handler.bigquery_handler import BigQueryHandler

        # connect to MindsDB BigQuery DB
        bq_handler = BigQueryHandler('bigquery', {
            'project_id': os.environ.get('MINDSDB_BIGQUERY_PROJECT_ID'),
            'dataset': os.environ.get('MINDSDB_BIGQUERY_DATASET'),
            'service_account_json': json.loads(os.environ.get('MINDSDB_BIGQUERY_SERVICE_ACCOUNT_JSON'))
        })

        # get top 10 most popular data sources and AI engines
        top_10_data_sources_query = """
            SELECT data_source_type
            FROM dataform_3_reports.model_stats
            WHERE data_source_type IS NOT NULL AND is_quickstart IS FALSE AND is_bot IS FALSE
            GROUP BY data_source_type
            ORDER BY COUNT(*) DESC
            LIMIT 10
        """
        top_10_ai_engines_query = """
            SELECT engine
            FROM dataform_3_reports.model_stats
            WHERE engine IS NOT NULL AND is_quickstart IS FALSE AND is_bot IS FALSE
            GROUP BY engine
            ORDER BY COUNT(*) DESC
            LIMIT 10
        """

        top_10_data_sources_df = bq_handler.native_query(top_10_data_sources_query).data_frame
        top_10_ai_engines_df = bq_handler.native_query(top_10_ai_engines_query).data_frame

        return top_10_data_sources_df['data_source_type'].tolist(), top_10_ai_engines_df['engine'].tolist()


@ns_conf.route('/<handler_name>/icon')
class HandlerIcon(Resource):
    @ns_conf.param('handler_name', 'Handler name')
    @api_endpoint_metrics('GET', '/handlers/handler/icon')
    def get(self, handler_name):
        try:
            handlers_import_status = ca.integration_controller.get_handlers_import_status()
            icon_name = handlers_import_status[handler_name]['icon']['name']
            handler_folder = handlers_import_status[handler_name]['import']['folder']
            mindsdb_path = Path(importlib.util.find_spec('mindsdb').origin).parent
            icon_path = mindsdb_path.joinpath('integrations/handlers').joinpath(handler_folder).joinpath(icon_name)
            if icon_path.is_absolute() is False:
                icon_path = Path(os.getcwd()).joinpath(icon_path)
        except Exception:
            return abort(404)
        else:
            return send_file(icon_path)


@ns_conf.route('/<handler_name>/install')
class InstallDependencies(Resource):
    @ns_conf.param('handler_name', 'Handler name')
    @api_endpoint_metrics('POST', '/handlers/handler/install')
    def post(self, handler_name):
        handler_import_status = ca.integration_controller.get_handlers_import_status()
        if handler_name not in handler_import_status:
            return f'Unknown handler: {handler_name}', 400

        if handler_import_status[handler_name].get('import', {}).get('success', False) is True:
            return 'Installed', 200

        handler_meta = handler_import_status[handler_name]

        dependencies = handler_meta['import']['dependencies']
        if len(dependencies) == 0:
            return 'Installed', 200

        result = install_dependencies(dependencies)

        # reload it if any result, so we can get new error message
        ca.integration_controller.reload_handler_module(handler_name)
        if result.get('success') is True:
            return '', 200
        return http_error(
            500,
            f'Failed to install dependencies for {handler_meta.get("title", handler_name)}',
            result.get('error_message', 'unknown error')
        )


def prepare_formdata():
    params = {}
    file_names = []

    def on_field(field):
        name = field.field_name.decode()
        value = field.value.decode()
        params[name] = value

    def on_file(file):
        params[file.field_name.decode()] = file.file_object
        file_names.append(file.field_name.decode())

    temp_dir_path = temp_dir_path = tempfile.mkdtemp(
        prefix='mindsdb_byom_file_',
        dir=Config().paths['tmp']
    )

    parser = multipart.create_form_parser(
        headers=request.headers,
        on_field=on_field,
        on_file=on_file,
        config={
            'UPLOAD_DIR': temp_dir_path.encode(),  # bytes required
            'UPLOAD_KEEP_FILENAME': True,
            'UPLOAD_KEEP_EXTENSIONS': True,
            'MAX_MEMORY_FILE_SIZE': 0
        }
    )

    while True:
        chunk = request.stream.read(8192)
        if not chunk:
            break
        parser.write(chunk)
    parser.finalize()
    parser.close()

    for file_name in file_names:
        params[file_name].close()

    return params


@ns_conf.route('/byom/<name>')
@ns_conf.param('name', "Name of the model")
class BYOMUpload(Resource):
    @ns_conf.doc('post_file')
    @api_endpoint_metrics('POST', '/handlers/byom/handler')
    def post(self, name):
        params = prepare_formdata()

        code_file_path = params['code'].name.decode()
        try:
            module_file_path = params['modules'].name.decode()
        except AttributeError:
            module_file_path = Path(code_file_path).parent / 'requirements.txt'
            module_file_path.touch()
            module_file_path = str(module_file_path)

        connection_args = {
            'code': code_file_path,
            'modules': module_file_path,
            'type': params.get('type')
        }

        session = SessionController()

        base_ml_handler = session.integration_controller.get_ml_handler(name)
        base_ml_handler.update_engine(connection_args)

        engine_storage = HandlerStorage(base_ml_handler.integration_id)

        engine_versions = [
            int(x) for x in engine_storage.get_connection_args()['versions'].keys()
        ]

        return {
            'last_engine_version': max(engine_versions),
            'engine_versions': engine_versions
        }

    @ns_conf.doc('put_file')
    @api_endpoint_metrics('PUT', '/handlers/byom/handler')
    def put(self, name):
        ''' upload new model
            params in FormData:
                - code
                - modules
        '''

        params = prepare_formdata()

        code_file_path = params['code'].name.decode()
        try:
            module_file_path = params['modules'].name.decode()
        except AttributeError:
            module_file_path = Path(code_file_path).parent / 'requirements.txt'
            module_file_path.touch()
            module_file_path = str(module_file_path)

        connection_args = {
            'code': code_file_path,
            'modules': module_file_path,
            'type': params.get('type')
        }

        ast_query = CreateMLEngine(
            name=Identifier(name),
            handler='byom',
            params=connection_args
        )
        sql_session = SessionController()
        command_executor = ExecuteCommands(sql_session)
        command_executor.execute_command(ast_query)

        return '', 200
