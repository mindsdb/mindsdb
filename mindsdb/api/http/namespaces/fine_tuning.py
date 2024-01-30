import uuid
from flask import request
from http import HTTPStatus
from flask_restx import Resource

from mindsdb.interfaces.storage.db import Predictor
from mindsdb.interfaces.model.functions import PredictorRecordNotFound
from mindsdb.interfaces.model.functions import get_predictor_integration

from mindsdb.api.http.utils import http_error
from mindsdb.api.http.namespaces.configs.fine_tuning import ns_conf
from mindsdb.api.executor.controllers.session_controller import SessionController


@ns_conf.route('/jobs')
class FineTuning(Resource):
    @ns_conf.doc('create_fine_tuning_job')
    def post(self):
        # extract parameters from request
        try:
            model = request.json['model']
            training_file = request.json['training_file']
        except KeyError:
            return 'The model and training_file parameters are required', HTTPStatus.BAD_REQUEST

        # TODO: add support for hyperparameters, suffix and validation_file
        hyperparameters = request.json.get('hyperparameters', None)
        suffix = request.json.get('suffix', None)
        validation_file = request.json.get('validation_file', None)

        # initialize session controller
        session = SessionController()

        project_name, model_name = model.split('.', 1)

        try:
            name_no_version, version = Predictor.get_name_and_version(model_name)
            try:
                model_record = session.model_controller.get_model_record(name_no_version, version=version, project_name=project_name)
            except PredictorRecordNotFound:
                return http_error(
                    HTTPStatus.NOT_FOUND,
                    'Model not found',
                    f'Model with name {model} not found')
            
            # get the integration handler
            integration_name = get_predictor_integration(model_record).name

            base_ml_engine = session.integration_controller.get_handler(integration_name)

            job_id = uuid.uuid4().hex

            base_ml_engine.finetune(
                model_name=model_record.name,
                base_model_version=model_record.version,
                project_name=project_name,
                data_integration_ref=model_record.data_integration_ref,
                fetch_data_query=f"SELECT * FROM {training_file}",
            )

            return {
                'job_id': job_id
            }, HTTPStatus.OK
        except Exception as e:
            return str(e), HTTPStatus.INTERNAL_SERVER_ERROR