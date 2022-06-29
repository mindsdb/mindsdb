import json
from dateutil.parser import parse as parse_datetime
from flask import request
from flask_restx import Resource, abort
from mindsdb.utilities.log import log
from mindsdb.api.http.utils import http_error
from mindsdb.api.http.namespaces.configs.handlers import ns_conf


# [{
#     name: 'postgres',
#     icon: {base64 string},
#     dependencies: ['psycopg', 'somethingelse>=1.0'],
#     import: {
#         success: True|False,
#         error_message: 'module xxx is not found'    // only if success=False
#     },
#     version: '0.1',
#     connection_example: {
#         'host': '127.0.0.1'.
#         'password': '1234',
#         'username': 'admin'
#     },
#     // to add in future?
#     description: 'connection to postgres',
#     connection_args: [{
#         name: 'host',
#         type: 'string',
#         example: '127.0.0.1',
#         description: 'host connect to'
#     }, ...]
# }]


@ns_conf.route('/')
class HandlersList(Resource):
    @ns_conf.doc('handlers_list')
    def get(self):
        '''List all db handlers'''
        handlers = request.integration_controller.get_handlers_import_status()
        result = []
        for handler_type, handler_meta in handlers.items():
            # pass not-data handlers
            if handler_type in ('file', 'view', 'ludwig', 'lightwood', 'mlflow'):
                continue
            row = {'type': handler_type}
            row.update(handler_meta)
            result.append(row)
        return result
