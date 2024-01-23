from flask import request
from flask_restx import Resource

from mindsdb.api.http.namespaces.configs.fine_tuning import ns_conf


@ns_conf.route('/')
class FineTuning(Resource):
    @ns_conf.doc('create_fine_tuning_job')
    def post(self):
        model = request.json['model']

        return {'model': model}