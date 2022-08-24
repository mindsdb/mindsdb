import os
import importlib
from pathlib import Path

from flask import request, send_file, abort
from flask_restx import Resource

from mindsdb.api.http.utils import http_error
from mindsdb.api.http.namespaces.configs.handlers import ns_conf
from mindsdb.integrations.utilities.install import install_dependencies


@ns_conf.route('/')
class HandlersList(Resource):
    @ns_conf.doc('handlers_list')
    def get(self):
        '''List all db handlers'''
        handlers = request.integration_controller.get_handlers_import_status()
        result = []
        for handler_type, handler_meta in handlers.items():
            row = {'name': handler_type}
            row.update(handler_meta)
            result.append(row)
        return result


@ns_conf.route('/<handler_name>/icon')
class HandlerIcon(Resource):
    @ns_conf.param('handler_name', 'Handler name')
    def get(self, handler_name):
        try:
            handlers_import_status = request.integration_controller.get_handlers_import_status()
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
    def post(self, handler_name):
        handler_import_status = request.integration_controller.get_handlers_import_status()
        if handler_name not in handler_import_status:
            return f'Unkown handler: {handler_name}', 400

        if handler_import_status[handler_name].get('import', {}).get('success', False) is True:
            return 'Installed', 200

        handler_meta = handler_import_status[handler_name]

        dependencies = handler_meta['import']['dependencies']
        if len(dependencies) == 0:
            return 'Installed', 200

        result = install_dependencies(dependencies)

        # reload it if any result, so we can get new error message
        request.integration_controller.reload_handler_module(handler_name)
        if result.get('success') is True:
            return '', 200
        return http_error(
            500,
            'Failed to install dependency',
            result.get('error_message', 'unknown error')
        )
