from flask import request
from flask_restx import Resource, abort
from flask import current_app as ca

from mindsdb.api.http.namespaces.configs.util import ns_conf
from mindsdb import __about__

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
