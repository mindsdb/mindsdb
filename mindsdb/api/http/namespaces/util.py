import os
from flask import request
from flask_restx import Resource, abort
from flask import current_app as ca

from mindsdb.api.http.namespaces.configs.util import ns_conf
from mindsdb import __about__

TELEMETRY_FILE = 'telemetry.lock'

@ns_conf.route('/ping')
class Ping(Resource):
    @ns_conf.doc('get_ping')
    def get(self):
        '''Checks server avaliable'''
        return {'status': 'ok'}

@ns_conf.route('/report_uuid')
class ReportUUID(Resource):
    @ns_conf.doc('get_report_uuid')
    def get(self):
        metamodel_name = '___monitroing_metamodel___'
        predictor = ca.mindsdb_native.create(metamodel_name)
        return {
            'report_uuid': predictor.report_uuid
        }

@ns_conf.route('/telemetry')
class Telemetry(Resource):
    @ns_conf.doc('get_telemetry_status')
    def get(self):
        status = "enabled" if is_telemetry_active() else "disabled"
        return {"status": status}

    @ns_conf.doc('set_telemetry')
    def post(self):
        data = request.json
        action = data['action']
        if str(action).lower() in ["true", "enable", "on"]:
            enable_telemetry()
        else:
            disable_telemetry()


def enable_telemetry():
    path = os.path.join(ca.config_obj['storage_dir'], TELEMETRY_FILE)
    if os.path.exists(path):
        os.remove(path)

def disable_telemetry():
    path = os.path.join(ca.config_obj['storage_dir'], TELEMETRY_FILE)
    with open(path, 'w') as _:
        pass

def is_telemetry_active():
    path = os.path.join(ca.config_obj['storage_dir'], TELEMETRY_FILE)
    return not os.path.exists(path)
