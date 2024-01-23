from flask import request
from http import HTTPStatus
from flask_restx import Resource

from mindsdb.api.http.namespaces.configs.fine_tuning import ns_conf
from mindsdb.api.executor.controllers.session_controller import SessionController


@ns_conf.route('/')
class FineTuning(Resource):
    @ns_conf.doc('create_fine_tuning_job')
    def post(self):
        try:
            model = request.json['model']
            training_file = request.json['training_file']
        except KeyError:
            return 'The model and training_file parameters are required', HTTPStatus.BAD_REQUEST

        hyperparameters = request.json.get('hyperparameters', None)
        suffix = request.json.get('suffix', None)
        validation_file = request.json.get('validation_file', None)

        session = SessionController()

        try:
            # TODO: create a new fine tuning job via model controller

            return 'Fine tuning job created', HTTPStatus.OK
        except Exception as e:
            return str(e), HTTPStatus.INTERNAL_SERVER_ERROR