from http import HTTPStatus

import json

from flask import request
from flask_restx import Resource
from sqlalchemy.exc import NoResultFound
import pandas as pd

from mindsdb.api.http.namespaces.configs.projects import ns_conf
from mindsdb.api.http.utils import http_error
from mindsdb.api.executor.controllers.session_controller import SessionController
from mindsdb.metrics.metrics import api_endpoint_metrics
from mindsdb.interfaces.model.functions import PredictorRecordNotFound
from mindsdb.interfaces.storage.db import Predictor
from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser.ast.mindsdb import CreatePredictor


@ns_conf.route('/<project_name>/models')
class ModelsList(Resource):
    @ns_conf.doc('list_models')
    @api_endpoint_metrics('GET', '/models')
    def get(self, project_name):
        ''' List all models '''
        session = SessionController()

        try:
            session.database_controller.get_project(project_name)
        except NoResultFound:
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project name {project_name} does not exist')

        return session.model_controller.get_models(with_versions=True, project_name=project_name)

    @ns_conf.doc('train_model')
    @api_endpoint_metrics('POST', '/models')
    def post(self, project_name):
        '''Creates a new model and trains it'''
        session = SessionController()

        if 'query' not in request.json:
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Query required',
                'Missing "query" SQL statement')
        query = request.json['query']

        project_datanode = session.datahub.get(project_name)
        if project_datanode is None:
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project name {project_name} does not exist')

        try:
            create_statement = parse_sql(query)
        except Exception:
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Invalid query string',
                f'SQL CREATE statement is invalid: {query}')

        if type(create_statement) is not CreatePredictor:
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Invalid CREATE SQL statement',
                f'SQL statement is not a CREATE model statement: {query}')

        model_name = create_statement.name.parts[1].lower()
        try:
            session.model_controller.get_model(model_name, project_name=project_name)
            return http_error(
                HTTPStatus.CONFLICT,
                'Model already exists',
                f'Model with name {model_name} already exists')
        except PredictorRecordNotFound:
            pass

        ml_integration = 'lightwood'
        if create_statement.using is not None:
            # Convert using to lowercase
            create_statement.using = {k.lower(): v for k, v in create_statement.using.items()}
            ml_integration = create_statement.using.pop("engine", ml_integration)

        try:
            ml_handler = session.integration_controller.get_ml_handler(ml_integration)
        except Exception:
            return http_error(
                HTTPStatus.NOT_FOUND,
                'ML handler not found',
                f'Cannot find ML handler with name {ml_integration}')

        try:
            model_df = session.model_controller.create_model(create_statement, ml_handler)
            # Consistent format with GET /projects/<project_name>/models/<model_name>
            return {
                'name': model_df.at[0, 'NAME'],
                'accuracy': None,
                'active': model_df.at[0, 'ACTIVE'],
                'version': model_df.at[0, 'VERSION'],
                'status': model_df.at[0, 'STATUS'],
                'predict': model_df.at[0, 'PREDICT'],
                'mindsdb_version': model_df.at[0, 'MINDSDB_VERSION'],
                'error': model_df.at[0, 'ERROR'],
                'fetch_data_query': model_df.at[0, 'SELECT_DATA_QUERY'],
                'problem_definition': model_df.at[0, 'TRAINING_OPTIONS']
            }, HTTPStatus.CREATED
        except Exception as e:
            return http_error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                'Unable to train model',
                f'Something went wrong while creating and training model {model_name}: {e}')


@ns_conf.route('/<project_name>/models/<model_name>')
@ns_conf.param('project_name', 'Name of the project')
@ns_conf.param('model_name', 'Name of the model')
class ModelResource(Resource):
    @ns_conf.doc('get_model')
    @api_endpoint_metrics('GET', '/models/model')
    def get(self, project_name, model_name):
        '''Get a model by name and version'''
        session = SessionController()

        project_datanode = session.datahub.get(project_name)
        if project_datanode is None:
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project name {project_name} does not exist')

        name_no_version, version = Predictor.get_name_and_version(model_name)
        try:
            return session.model_controller.get_model(name_no_version, version=version, project_name=project_name)
        except PredictorRecordNotFound:
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Model not found',
                f'Model with name {model_name} not found')

    @ns_conf.doc('update_model')
    @api_endpoint_metrics('PUT', '/models/model')
    def put(self, project_name, model_name):
        """Update model"""

        session = SessionController()

        project_datanode = session.datahub.get(project_name)
        if project_datanode is None:
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project name {project_name} does not exist')

        if 'problem_definition' not in request.json:
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'problem_definition required',
                'Missing "problem_definition" field')

        problem_definition = request.json['problem_definition']

        model_name, version = Predictor.get_name_and_version(model_name)

        session.model_controller.update_model(
            session,
            project_name,
            model_name,
            version=version,
            problem_definition=problem_definition,
        )
        return session.model_controller.get_model(model_name, version=version, project_name=project_name)

    @ns_conf.doc('delete_model')
    @api_endpoint_metrics('DELETE', '/models/model')
    def delete(self, project_name, model_name):
        '''Deletes a model by name'''

        session = SessionController()

        project_datanode = session.datahub.get(project_name)
        if project_datanode is None:
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project name {project_name} does not exist')

        name_no_version, version = Predictor.get_name_and_version(model_name)
        try:
            session.model_controller.get_model(name_no_version, version=version, project_name=project_name)
        except PredictorRecordNotFound:
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Model not found',
                f'Model with name {model_name} not found')

        try:
            session.model_controller.delete_model(name_no_version, project_name, version=version)
        except Exception as e:
            return http_error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                'Error deleting model',
                f'Something went wrong while deleting {model_name}: {e}')

        return '', HTTPStatus.NO_CONTENT


@ns_conf.route('/<project_name>/models/<model_name>/predict')
@ns_conf.param('project_name', 'Name of the project')
@ns_conf.param('model_name', 'Name of the model')
class ModelPredict(Resource):
    @ns_conf.doc('post_model_predict')
    @api_endpoint_metrics('POST', '/models/model/predict')
    def post(self, project_name, model_name):
        '''Call prediction'''

        name_no_version, version = Predictor.get_name_and_version(model_name)

        session = SessionController()
        project_datanode = session.datahub.get(project_name)
        if project_datanode is None:
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project with name {project_name} does not exist')

        try:
            session.model_controller.get_model(name_no_version, version=version, project_name=project_name)
        except PredictorRecordNotFound:
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Model not found',
                f'Model with name {model_name} not found')

        data = request.json['data']
        if isinstance(data, str):
            # Support object or serialized object.
            data = json.loads(data)
        params = request.json.get('params')

        predictions = project_datanode.predict(
            model_name=name_no_version,
            df=pd.DataFrame(data),
            version=version,
            params=params,
        )

        return predictions.to_dict('records')


@ns_conf.route('/<project_name>/models/<model_name>/describe')
@ns_conf.param('project_name', 'Name of the project')
@ns_conf.param('model_name', 'Name of the model')
class ModelDescribe(Resource):
    @ns_conf.doc('describe_model')
    @api_endpoint_metrics('GET', '/models/model/describe')
    def get(self, project_name, model_name):
        '''Describes a model'''
        session = SessionController()

        project_datanode = session.datahub.get(project_name)
        if project_datanode is None:
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project name {project_name} does not exist')

        name_no_version, version = Predictor.get_name_and_version(model_name)

        try:
            session.model_controller.get_model(name_no_version, version=version, project_name=project_name)
        except PredictorRecordNotFound:
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Model not found',
                f'Model with name {model_name} not found')

        attribute = None
        if 'attribute' in request.json:
            attribute = request.json['attribute']

        try:
            description_df = session.model_controller.describe_model(
                session,
                project_name,
                name_no_version,
                attribute,
                version=version)
            return description_df.to_dict('records')
        except Exception:
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'ML handler unsupported',
                f'ML handler for {model_name} does not support model description')
