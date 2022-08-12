import json
from dateutil.parser import parse as parse_datetime

from flask import request
from flask_restx import Resource, abort

from mindsdb_sql.parser.dialects.mindsdb import CreatePredictor
from mindsdb_sql.parser.ast import Identifier

from mindsdb.utilities.log import log
from mindsdb.api.http.utils import http_error
from mindsdb.api.http.namespaces.configs.predictors import ns_conf
from mindsdb.api.http.namespaces.entitites.predictor_metadata import (
    predictor_query_params,
    put_predictor_params,
    put_predictor_metadata
)
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


@ns_conf.route('/')
class PredictorList(Resource):
    @ns_conf.doc('list_predictors')
    def get(self):
        '''List all predictors'''
        models = request.model_controller.get_models()
        return models


@ns_conf.route('/<name>')
@ns_conf.param('name', 'The predictor identifier')
@ns_conf.response(404, 'predictor not found')
class Predictor(Resource):
    @ns_conf.doc('get_predictor')
    def get(self, name):
        model = request.model_controller.get_model_data(name)

        for k in ['train_end_at', 'updated_at', 'created_at']:
            if k in model and model[k] is not None:
                model[k] = parse_datetime(model[k])

        return model

    @ns_conf.doc('delete_predictor')
    def delete(self, name):
        '''Remove predictor'''
        request.model_controller.delete_model(name)
        return '', 200

    @ns_conf.doc('put_predictor')
    @ns_conf.expect(put_predictor_metadata)
    def put(self, name):
        '''Learning new predictor'''
        lw_handler = request.integration_controller.get_handler('lightwood')

        data = request.json

        try:
            kwargs = data.get('kwargs')
        except Exception:
            kwargs = None

        if isinstance(kwargs, dict) is False:
            kwargs = {}

        to_predict = data.get('to_predict')
        if isinstance(to_predict, list) is False:
            to_predict = [to_predict]

        ast = CreatePredictor(
            name=Identifier(name),
            integration_name=Identifier(data.get('integration')),
            query_str=data.get('query'),
            targets=[Identifier(x) for x in to_predict]
            # TODO add ts settings
        )

        response = lw_handler.query(ast)
        if response.type == RESPONSE_TYPE.ERROR:
            return http_error(400, detail=response.error_message)
        return '', 200


@ns_conf.route('/<name>/update')
@ns_conf.param('name', 'Update predictor')
class PredictorUpdate(Resource):
    @ns_conf.doc('Update predictor')
    def get(self, name):
        lw_handler = request.integration_controller.get_handler('lightwood')
        response = lw_handler.native_query(f'retrain {name}')
        if response.type == RESPONSE_TYPE.ERROR:
            return http_error(400, detail=response.error_message)
        return '', 200


@ns_conf.route('/<name>/adjust')
@ns_conf.param('name', 'The predictor identifier')
class PredictorAdjust(Resource):
    @ns_conf.doc('post_predictor_adjust', params=predictor_query_params)
    def post(self, name):
        return abort(410, 'Method is not available')


@ns_conf.route('/<name>/predict')
@ns_conf.param('name', 'The predictor identifier')
class PredictorPredict(Resource):
    @ns_conf.doc('post_predictor_predict', params=predictor_query_params)
    def post(self, name):
        '''Queries predictor'''
        when = request.json.get('when')

        # list is a required type for TS prediction
        if isinstance(when, (dict, list)) is False or len(when) == 0:
            return 'No data provided for the predictions', 400
        if isinstance(when, dict):
            when = [when]

        # results = request.model_controller.predict(name, when, 'explain')
        lw_handler = request.integration_controller.get_handler('lightwood')
        response = lw_handler.predict(name, when, pred_format='explain')
        return response


@ns_conf.route('/<name>/rename')
@ns_conf.param('name', 'The predictor identifier')
class PredictorDownload(Resource):
    @ns_conf.doc('get_predictor_download')
    def get(self, name):
        try:
            new_name = request.args.get('new_name')
            request.model_controller.rename_model(name, new_name)
        except Exception as e:
            return str(e), 400

        return f'Renamed model to {new_name}', 200


@ns_conf.route('/generate/<name>')
@ns_conf.param('name', 'The predictor identifier')
@ns_conf.response(404, 'predictor not found')
class PredictorGenerate(Resource):
    def put(self, name):
        return abort(410, 'Method is not available')
        # problem_definition = request.json['problem_definition']
        # datasource_name = request.json['data_source_name']

        # from_data = request.default_store.get_datasource_obj(
        #     datasource_name,
        #     raw=True
        # )
        # datasource = request.default_store.get_datasource(datasource_name)

        # request.model_controller.generate_predictor(
        #     name,
        #     from_data,
        #     datasource['id'],
        #     problem_definition,
        #     request.json.get('join_learn_process', False)
        # )

        # return '', 200


@ns_conf.route('/<name>/edit/json_ai')
@ns_conf.param('name', 'The predictor identifier')
@ns_conf.response(404, 'predictor not found')
class PredictorEditJsonAI(Resource):
    def put(self, name):
        request.model_controller.edit_json_ai(name, request.json['json_ai'])
        return '', 200


@ns_conf.route('/<name>/edit/code')
@ns_conf.param('name', 'The predictor identifier')
@ns_conf.response(404, 'predictor not found')
class PredictorEditCode(Resource):
    def put(self, name):
        request.model_controller.edit_code(name, request.json['code'])
        return '', 200


@ns_conf.route('/<name>/train')
@ns_conf.param('name', 'The predictor identifier')
@ns_conf.response(404, 'predictor not found')
class PredictorTrain(Resource):
    def put(self, name):
        return abort(410, 'Method is not available')

        # for param in ['data_source_name']:
        #     if param not in request.json:
        #         return abort(400, 'Please provide {}'.format(param))

        # from_data = request.default_store.get_datasource_obj(
        #     request.json['data_source_name'],
        #     raw=True
        # )

        # request.model_controller.fit_predictor(name, from_data, request.json.get('join_learn_process', False))
        # return '', 200


@ns_conf.route('/<name>/export')
@ns_conf.param('name', 'The predictor identifier')
@ns_conf.response(404, 'predictor not found')
class PredictorExport(Resource):
    def get(self, name):
        payload: json = request.model_controller.export_predictor(name)
        return payload, 200


@ns_conf.route('/<name>/import')
@ns_conf.param('name', 'The predictor identifier')
@ns_conf.response(404, 'predictor not found')
class PredictorImport(Resource):
    def put(self, name):
        request.model_controller.import_predictor(name, request.json)
        return '', 200
