from flask import request
from http import HTTPStatus
from flask_restx import Resource

from mindsdb_sql.parser.ast import Identifier
from mindsdb_sql.parser.dialects.mindsdb import FinetunePredictor

from mindsdb.interfaces.storage.db import Predictor
from mindsdb.interfaces.model.functions import PredictorRecordNotFound
from mindsdb.interfaces.model.functions import get_predictor_integration

from mindsdb.api.http.utils import http_error
from mindsdb.api.http.namespaces.configs.fine_tuning import ns_conf
from mindsdb.api.executor.controllers.session_controller import SessionController


@ns_conf.route('/')
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

        try:
            name_no_version, version = Predictor.get_name_and_version(model.split('.')[1])
            try:
                model_record = session.model_controller.get_model_record(name_no_version, version=version, project_name=model.split('.')[0])
            except PredictorRecordNotFound:
                return http_error(
                    HTTPStatus.NOT_FOUND,
                    'Model not found',
                    f'Model with name {model} not found')
            
            # get the integration handler
            integration_name = get_predictor_integration(model_record).name

            ml_handler = session.integration_controller.get_handler(
                integration_name
            )

            # create a fine tuning query
            ast_query = FinetunePredictor(
                name=Identifier(model),
                integration_name=Identifier('files'),
                query_str=f"SELECT * FROM {training_file}",
            )

            # run the query using the model controller
            session.model_controller.finetune_model(ast_query, ml_handler)

            return 'Fine tuning job created', HTTPStatus.OK
        except Exception as e:
            return str(e), HTTPStatus.INTERNAL_SERVER_ERROR