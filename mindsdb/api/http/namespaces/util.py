from pathlib import Path
import re
import datetime

from flask import request
from flask import current_app as ca
from flask_restx import Resource, abort

from mindsdb.api.http.namespaces.configs.util import ns_conf
from mindsdb.api.http.namespaces.entitites.log import log as log_metadata


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


log_regexp = re.compile(r'(.+)\s\-\s([a-zA-Z]+)\s\-\s(.*)')


@ns_conf.route('/log')
class Log(Resource):
    @ns_conf.doc('get_log')
    @ns_conf.marshal_list_with(log_metadata)
    def get(self):
        '''Get all log messages'''
        logs = []
        log_path = Path(ca.config_obj.paths['log'])
        for x in log_path.iterdir():
            if x.is_dir():
                api_name = x.name
                if x.joinpath('log.txt').is_file():
                    try:
                        with open(str(x.joinpath('log.txt')), 'rt') as f:
                            for line in f:
                                res = log_regexp.findall(line)
                                dt = datetime.datetime.strptime(res[0][0], '%Y-%m-%d %H:%M:%S,%f')
                                level = res[0][1]
                                msg = res[0][2]
                                logs.append([api_name, dt, level, msg])
                    except Exception:
                        pass

        logs.sort(key=lambda x: x[1], reverse=True)

        logs = [{
            'source': x[0],
            'date': x[1].timestamp(),
            'level': x[2],
            'msg': x[3]
        } for x in logs[:100]]

        return logs, 200
