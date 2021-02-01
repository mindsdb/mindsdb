from flask import request
from flask_restx import Resource
from flask import current_app as ca

from mindsdb.api.http.namespaces.configs.util import ns_conf
from mindsdb.utilities.telemetry import (
    enable_telemetry,
    disable_telemetry,
    is_telemetry_file_exists,
    inject_telemetry_to_static
)


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
        storage_dir = ca.config_obj['storage_dir']
        status = "enabled" if is_telemetry_file_exists(storage_dir) else "disabled"
        return {"status": status}

    @ns_conf.doc('set_telemetry')
    def post(self):
        data = request.json
        action = data['action']
        storage_dir = ca.config_obj['storage_dir']
        if str(action).lower() in ["true", "enable", "on"]:
            enable_telemetry(storage_dir)
        else:
            disable_telemetry(storage_dir)
        inject_telemetry_to_static(ca.config_obj.paths['static'])
        return '', 200
