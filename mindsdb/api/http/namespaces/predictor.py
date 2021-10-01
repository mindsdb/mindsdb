import time
from dateutil.parser import parse as parse_datetime
from flask import request
from flask_restx import Resource, abort
from mindsdb.utilities.log import log
from mindsdb.api.http.utils import http_error
from mindsdb.api.http.namespaces.configs.predictors import ns_conf
from mindsdb.api.http.namespaces.entitites.predictor_metadata import (
    predictor_query_params,
    put_predictor_params
)

@ns_conf.route('/')
class PredictorList(Resource):
    @ns_conf.doc('list_predictors')
    def get(self):
        '''List all predictors'''
        models = request.model_interface.get_models()
        return models

@ns_conf.route('/<name>')
@ns_conf.param('name', 'The predictor identifier')
@ns_conf.response(404, 'predictor not found')
class Predictor(Resource):
    @ns_conf.doc('get_predictor')
    def get(self, name):
        model = request.model_interface.get_model_data(name)

        for k in ['train_end_at', 'updated_at', 'created_at']:
            if k in model and model[k] is not None:
                model[k] = parse_datetime(model[k])

        return model

    @ns_conf.doc('delete_predictor')
    def delete(self, name):
        '''Remove predictor'''
        request.model_interface.delete_model(name)
        return '', 200

    @ns_conf.doc('put_predictor', params=put_predictor_params)
    def put(self, name):
        '''Learning new predictor'''
        data = request.json
        to_predict = data.get('to_predict')

        try:
            kwargs = data.get('kwargs')
        except Exception:
            kwargs = None

        if isinstance(kwargs, dict) is False:
            kwargs = {}

        if 'equal_accuracy_for_all_output_categories' not in kwargs:
            kwargs['equal_accuracy_for_all_output_categories'] = True

        if 'advanced_args' not in kwargs:
            kwargs['advanced_args'] = {}

        if 'use_selfaware_model' not in kwargs['advanced_args']:
            kwargs['advanced_args']['use_selfaware_model'] = False

        try:
            retrain = data.get('retrain')
            if retrain in ('true', 'True'):
                retrain = True
            else:
                retrain = False
        except Exception:
            retrain = None

        ds_name = data.get('data_source_name') if data.get('data_source_name') is not None else data.get('from_data')
        from_data = request.default_store.get_datasource_obj(ds_name, raw=True)

        if from_data is None:
            return {'message': f'Can not find datasource: {ds_name}'}, 400

        if retrain is True:
            original_name = name
            name = name + '_retrained'

        model_names = [x['name'] for x in request.model_interface.get_models()]
        if name in model_names:
            return http_error(
                409,
                f"Predictor '{name}' already exists",
                f"Predictor with name '{name}' already exists. Each predictor must have unique name."
            )

        request.model_interface.learn(name, from_data, to_predict, request.default_store.get_datasource(ds_name)['id'], kwargs=kwargs)
        for i in range(20):
            try:
                # Dirty hack, we should use a messaging queue between the predictor process and this bit of the code
                request.model_interface.get_model_data(name)
                break
            except Exception:
                time.sleep(1)

        if retrain is True:
            try:
                request.model_interface.delete_model(original_name)
                request.model_interface.rename_model(name, original_name)
            except Exception:
                pass

        return '', 200


@ns_conf.route('/<name>/update')
@ns_conf.param('name', 'Update predictor')
class PredictorUpdate(Resource):
    @ns_conf.doc('Update predictor')
    def get(self, name):
        msg = request.model_interface.update_model(name)
        return {
            'message': msg
        }


@ns_conf.route('/<name>/adjust')
@ns_conf.param('name', 'The predictor identifier')
class PredictorAdjust(Resource):
    @ns_conf.doc('post_predictor_adjust', params=predictor_query_params)
    def post(self, name):
        data = request.json

        ds_name = data.get('data_source_name') if data.get('data_source_name') is not None else data.get('from_data')
        from_data = request.default_store.get_datasource_obj(ds_name, raw=True)

        if from_data is None:
            return {'message': f'Can not find datasource: {ds_name}'}, 400

        model_names = [x['name'] for x in request.model_interface.get_models()]
        if name not in model_names:
            return abort(404, f'Predictor "{name}" doesn\'t exist',)

        request.model_interface.adjust(
            name,
            from_data,
            request.default_store.get_datasource(ds_name)['id']
        )

        return '', 200


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

        results = request.model_interface.predict(name, when, 'explain')
        return results


@ns_conf.route('/<name>/predict_datasource')
@ns_conf.param('name', 'The predictor identifier')
class PredictorPredictFromDataSource(Resource):
    @ns_conf.doc('post_predictor_predict', params=predictor_query_params)
    def post(self, name):
        data = request.json
        use_raw = False

        from_data = request.default_store.get_datasource_obj(data.get('data_source_name'), raw=use_raw)
        if from_data is None:
            abort(400, 'No valid datasource given')

        results = request.model_interface.predict(name, from_data, 'explain')
        return results


@ns_conf.route('/<name>/rename')
@ns_conf.param('name', 'The predictor identifier')
class PredictorDownload(Resource):
    @ns_conf.doc('get_predictor_download')
    def get(self, name):
        try:
            new_name = request.args.get('new_name')
            request.model_interface.rename_model(name, new_name)
        except Exception as e:
            return str(e), 400

        return f'Renamed model to {new_name}', 200


@ns_conf.route('/generate/<name>')
@ns_conf.param('name', 'The predictor identifier')
@ns_conf.response(404, 'predictor not found')
class PredictorGenerate(Resource):
    def put(self, name):
        problem_definition = request.json['problem_definition']
        datasource_name = request.json['data_source_name']

        from_data = request.default_store.get_datasource_obj(
            datasource_name,
            raw=True
        )
        datasource = request.default_store.get_datasource(datasource_name)

        request.model_interface.generate_predictor(
            name,
            from_data,
            datasource['id'],
            problem_definition,
            request.json.get('join_learn_process', False)
        )

        return '', 200


@ns_conf.route('/<name>/edit/json_ai')
@ns_conf.param('name', 'The predictor identifier')
@ns_conf.response(404, 'predictor not found')
class PredictorEditJsonAI(Resource):
    def put(self, name):
        request.model_interface.edit_json_ai(name, request.json['json_ai'])
        return '', 200


@ns_conf.route('/<name>/edit/code')
@ns_conf.param('name', 'The predictor identifier')
@ns_conf.response(404, 'predictor not found')
class PredictorEditCode(Resource):
    def put(self, name):
        request.model_interface.edit_code(name, request.json['code'])
        return '', 200
    

@ns_conf.route('/<name>/train')
@ns_conf.param('name', 'The predictor identifier')
@ns_conf.response(404, 'predictor not found')
class PredictorTrain(Resource):
    def put(self, name):
        for param in ['data_source_name']:
            if param not in request.json:
                return abort(400, 'Please provide {}'.format(param))

        from_data = request.default_store.get_datasource_obj(
            request.json['data_source_name'],
            raw=True
        )

        request.model_interface.fit_predictor(name, from_data, request.json.get('join_learn_process', False))
        return '', 200
