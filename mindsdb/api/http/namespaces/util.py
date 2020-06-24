from flask import request
from flask_restx import Resource, abort

from mindsdb.api.http.namespaces.configs.util import ns_conf


@ns_conf.route('/ping')
class Ping(Resource):
    @ns_conf.doc('get_ping')
    def get(self):
        '''Checks server avaliable'''
        return {'status': 'ok'}


@ns_conf.route('/shutdown')
class Shutdown(Resource):
    @ns_conf.doc('get_shutdown')
    def get(self):
        '''Shutdown server'''
        if request.host.startswith('127.0.0.1') or request.host.startswith('localhost'):
            func = request.environ.get('werkzeug.server.shutdown')
            if func is None:
                return '', 500
            func()
            return '', 200
        abort(403, "")
