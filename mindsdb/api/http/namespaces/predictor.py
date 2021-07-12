import os
import time

from dateutil.parser import parse as parse_datetime
from flask import request
from flask_restx import Resource, abort
from flask import current_app as ca

from mindsdb.utilities.log import log
from mindsdb.api.http.utils import http_error
from mindsdb.api.http.namespaces.configs.predictors import ns_conf
from mindsdb.api.http.namespaces.entitites.predictor_metadata import (
    predictor_metadata,
    predictor_query_params,
    upload_predictor_params,
    put_predictor_params
)
from mindsdb.api.http.namespaces.entitites.predictor_status import predictor_status


@ns_conf.route('/')
class PredictorList(Resource):
    @ns_conf.doc('list_predictors')
    @ns_conf.marshal_list_with(predictor_status, skip_none=True)
    def get(self):
        '''List all predictors'''
        return request.native_interface.get_models()


@ns_conf.route('/custom/<name>')
@ns_conf.param('name', 'The predictor identifier')
@ns_conf.response(404, 'predictor not found')
class CustomPredictor(Resource):
    @ns_conf.doc('put_custom_predictor')
    def put(self, name):
        try:
            trained_status = request.json['trained_status']
        except Exception:
            trained_status = 'untrained'

        predictor_file = request.files['file']
        fpath = os.path.join(ca.config_obj.paths['tmp'],  name + '.zip')
        with open(fpath, 'wb') as f:
            f.write(predictor_file.read())

        request.custom_models.load_model(fpath, name, trained_status)

        return f'Uploaded custom model {name}'


@ns_conf.route('/<name>')
@ns_conf.param('name', 'The predictor identifier')
@ns_conf.response(404, 'predictor not found')
class Predictor(Resource):
    @ns_conf.doc('get_predictor')
    @ns_conf.marshal_with(predictor_metadata, skip_none=True)
    def get(self, name):
        return request.native_interface.get_model_data(name)

    @ns_conf.doc('delete_predictor')
    def delete(self, name):
        request.native_interface.delete_model(name)
        return '', 200

    @ns_conf.doc('put_predictor', params=put_predictor_params)
    def put(self, name):
        '''Learning new predictor'''

        if 'data_source_name' not in request.json and 'from_data' not in request.json:
            return abort(400, 'Please provide either "data_source_name" or "from_data"')

        if 'to_predict' not in request.json:
            return abort(400, 'Please provide "to_predict"')

        kwargs = request.json.get('kwargs')
        if not isinstance(kwargs, dict):
            kwargs = {}

        ds_name = request.json.get('data_source_name') if request.json.get('data_source_name') is not None else request.json.get('from_data')
        from_data = request.default_store.get_datasource_obj(ds_name, raw=True)

        if from_data is None:
            return {'message': f'Can not find datasource: {ds_name}'}, 400

        model_names = [x['name'] for x in request.native_interface.get_models()]
        if name in model_names:
            return http_error(
                409,
                f"Predictor '{name}' already exists",
                f"Predictor with name '{name}' already exists. Each predictor must have unique name."
            )

        problem_definition = {'target': request.json.get('to_predict')}

        if 'timeseries_settings' in kwargs:
            problem_definition['timeseries_settings'] = kwargs['timeseries_settings']
        
        if 'stop_training_in_x_seconds' in kwargs:
            problem_definition['stop_after'] = kwargs['stop_training_in_x_seconds']

        request.native_interface.generate_lightwood_predictor(name, from_data, problem_definition)
        request.native_interface.fit_predictor(name, from_data)

        return '', 200


@ns_conf.route('/<name>/learn')
@ns_conf.param('name', 'The predictor identifier')
class PredictorLearn(Resource):
    def post(self, name):
        data = request.json
        to_predict = data.get('to_predict')
        kwargs = data.get('kwargs', None)

        if not isinstance(kwargs, dict):
            kwargs = {}

        if 'advanced_args' not in kwargs:
            kwargs['advanced_args'] = {}

        ds_name = data.get('data_source_name') if data.get('data_source_name') is not None else data.get('from_data')
        from_data = request.default_store.get_datasource_obj(ds_name, raw=True)

        request.custom_models.learn(name, from_data, to_predict, request.default_store.get_datasource(ds_name)['id'], kwargs)

        return '', 200


@ns_conf.route('/<name>/update')
@ns_conf.param('name', 'Update predictor')
class PredictorPredict(Resource):
    @ns_conf.doc('Update predictor')
    def get(self, name):
        msg = request.native_interface.update_model(name)
        return {'message': msg}


@ns_conf.route('/<name>/predict')
@ns_conf.param('name', 'The predictor identifier')
class PredictorPredict2(Resource):
    @ns_conf.doc('post_predictor_predict', params=predictor_query_params)
    def post(self, name):
        '''Queries predictor'''
        data = request.json
        when = data.get('when', {})
        format_flag = data.get('format_flag', 'explain')
        kwargs = data.get('kwargs', {})

        if when is None:
            return 'No data provided for the predictions', 500

        results = request.native_interface.predict(name, format_flag, when_data=when, **kwargs)

        return results


@ns_conf.route('/<name>/predict_datasource')
@ns_conf.param('name', 'The predictor identifier')
class PredictorPredictFromDataSource(Resource):
    @ns_conf.doc('post_predictor_predict', params=predictor_query_params)
    def post(self, name):
        data = request.json
        format_flag = data.get('format_flag', 'explain')
        kwargs = data.get('kwargs', {})

        use_raw = False

        from_data = request.default_store.get_datasource_obj(data.get('data_source_name'), raw=use_raw)
        if from_data is None:
            abort(400, 'No valid datasource given')

        results = request.native_interface.predict(name, format_flag, when_data=from_data, **kwargs)
        return results


@ns_conf.route('/<name>/rename')
@ns_conf.param('name', 'The predictor identifier')
class PredictorDownload(Resource):
    @ns_conf.doc('get_predictor_download')
    def get(self, name):
        '''Export predictor to file'''
        try:
            new_name = request.args.get('new_name')
            request.native_interface.rename_model(name, new_name)
        except Exception as e:
            return str(e), 400

        return f'Renamed model to {new_name}', 200


@ns_conf.route('/lwr/generate/<name>')
@ns_conf.param('name', 'The predictor identifier')
@ns_conf.response(404, 'predictor not found')
class LWR_Generate(Resource):
    def put(self, name):
        for param in ['data_source_name', 'problem_definition']:
            if param not in request.json:
                return abort(400, 'Please provide {}'.format(param))

        from_data = request.default_store.get_datasource_obj(
            request.json['data_source_name'],
            raw=True
        )

        request.native_interface.generate_lightwood_predictor(
            name,
            from_data,
            request.json['problem_definition']
        )

        return '', 200


@ns_conf.route('/lwr/jsonai/edit/<name>')
@ns_conf.param('name', 'The predictor identifier')
@ns_conf.response(404, 'predictor not found')
class LWR_EditJsonAI(Resource):
    def put(self, name):
        for param in ['json_ai']:
            if param not in request.json:
                return abort(400, 'Please provide {}'.format(param))

        request.native_interface.edit_json_ai(name, request.json['json_ai'])
        return '', 200


@ns_conf.route('/lwr/code/edit/<name>')
@ns_conf.param('name', 'The predictor identifier')
@ns_conf.response(404, 'predictor not found')
class LWR_EditCode(Resource):
    def put(self, name):
        for param in ['code']:
            if param not in request.json:
                return abort(400, 'Please provide {}'.format(param))

        request.native_interface.edit_code(name, request.json['code'])
        return '', 200
    

@ns_conf.route('/lwr/train/<name>')
@ns_conf.param('name', 'The predictor identifier')
@ns_conf.response(404, 'predictor not found')
class LWR_Train(Resource):
    def put(self, name):
        for param in ['data_source_name']:
            if param not in request.json:
                return abort(400, 'Please provide {}'.format(param))

        from_data = request.default_store.get_datasource_obj(
            request.json['data_source_name'],
            raw=True
        )

        request.native_interface.fit_predictor(name, from_data)
        return '', 200


@ns_conf.route('/code_from_json_ai')
@ns_conf.param('name', 'The predictor identifier')
@ns_conf.response(404, 'predictor not found')
class LWR_Train(Resource):
    def get(self):
        for param in ['json_ai']:
            if param not in request.json:
                return abort(400, 'Please provide {}'.format(param))
        return {'code': request.native_interface.code_from_json_ai(request.json['json_ai'])}
