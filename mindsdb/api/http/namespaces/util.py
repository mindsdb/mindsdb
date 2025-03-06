import os
import tempfile
from pathlib import Path

import psutil
from flask import request
from flask_restx import Resource
from flask import current_app as ca

from mindsdb.metrics.metrics import api_endpoint_metrics
from mindsdb.api.http.namespaces.configs.util import ns_conf
from mindsdb.api.http.gui import update_static
from mindsdb.utilities.fs import clean_unlinked_process_marks
from mindsdb.api.http.utils import http_error


def get_active_tasks():
    response = {
        'learn': False,
        'predict': False,
        'analyse': False
    }

    if os.name != 'posix':
        return response

    for process_type in response:
        processes_dir = Path(tempfile.gettempdir()).joinpath(f'mindsdb/processes/{process_type}/')
        if not processes_dir.is_dir():
            continue
        clean_unlinked_process_marks()
        process_marks = [x.name for x in processes_dir.iterdir()]
        if len(process_marks) > 0:
            response[process_type] = True

    return response


@ns_conf.route('/ping')
class Ping(Resource):
    @ns_conf.doc('get_ping')
    @api_endpoint_metrics('GET', '/util/ping')
    def get(self):
        '''Checks server avaliable'''
        return {'status': 'ok'}


@ns_conf.route('/ping/ml_task_queue')
class PingMLTaskQueue(Resource):
    @ns_conf.doc('get_ping_ml_task_queue')
    @api_endpoint_metrics('GET', '/util/ping/ml_task_queue')
    def get(self):
        '''Check if ML tasks queue process is alive'''
        processes_dir = Path(tempfile.gettempdir()).joinpath('mindsdb/processes/internal/')
        if processes_dir.is_dir():
            ml_tasks_queue_mark = next((x for x in processes_dir.iterdir() if x.name.endswith('ml_task_consumer')), None)
            if ml_tasks_queue_mark is not None:
                try:
                    pid = int(ml_tasks_queue_mark.name.split('-')[0])
                    process = psutil.Process(pid)
                    if process.status() in (psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD):
                        raise psutil.NoSuchProcess(pid)
                    return '', 200
                except Exception:
                    return '', 404
        return '', 404


@ns_conf.route('/readiness')
class ReadinessProbe(Resource):
    @ns_conf.doc('get_ready')
    @api_endpoint_metrics('GET', '/util/readiness')
    def get(self):
        '''Checks server is ready for work'''

        tasks = get_active_tasks()
        for key in tasks:
            if tasks[key] is True:
                return http_error(503, 'not ready', 'not ready')

        return '', 200


@ns_conf.route('/ping_native')
class PingNative(Resource):
    @ns_conf.doc('get_ping_native')
    @api_endpoint_metrics('GET', '/util/ping_native')
    def get(self):
        ''' Checks server use native for learn or analyse.
            Will return right result only on Linux.
        '''
        return get_active_tasks()


@ns_conf.route('/validate_json_ai')
class ValidateJsonAI(Resource):
    @api_endpoint_metrics('POST', '/util/validate_json_ai')
    def post(self):
        json_ai = request.json.get('json_ai')
        if json_ai is None:
            return 'Please provide json_ai', 400
        try:
            lw_handler = ca.integration_controller.get_ml_handler('lightwood')
            code = lw_handler.code_from_json_ai(json_ai)
        except Exception as e:
            return {'error': str(e)}
        return {'code': code}


@ns_conf.route('/update-gui')
class UpdateGui(Resource):
    @ns_conf.doc('get_update_gui')
    @api_endpoint_metrics('GET', '/util/update-gui')
    def get(self):
        update_static()
        return '', 200
