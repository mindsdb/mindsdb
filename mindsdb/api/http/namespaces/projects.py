from flask import request
from flask_restx import Resource, abort

from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController

from mindsdb.api.http.namespaces.configs.projects import ns_conf


@ns_conf.route('/')
class ProjectsList(Resource):
    @ns_conf.doc('list_projects')
    def get(self):
        ''' List all projects '''
        session = SessionController()

        projects = [
            {'name': i}
            for i in session.datahub.get_projects_names()
        ]
        return projects


@ns_conf.route('/<project_name>/models')
class ModelsList(Resource):
    @ns_conf.doc('list_models')
    def get(self, project_name):
        ''' List all models '''
        session = SessionController()

        models = session.model_controller.get_models(project_name=project_name)
        return models


@ns_conf.route('/<project_name>/models/<model_name>/predict')
@ns_conf.param('project_name', 'Name of the project')
@ns_conf.param('predictor_name', 'Name of the model')
class ModelPredict(Resource):
    @ns_conf.doc('post_model_predict')
    def post(self, project_name, model_name):
        '''Call prediction'''

        # predictor version
        version = None
        parts = model_name.split('.')
        if len(parts) > 1 and parts[-1].isdigit():
            version = int(parts[-1])
            model_name = '.'.join(parts[:-1])

        data = request.json['data']
        params = request.json.get('params')

        session = SessionController()
        project_datanode = session.datahub.get(project_name)

        if project_datanode is None:
            abort(500, f'Project not found: {project_name}')

        predictions = project_datanode.predict(
            model_name=model_name,
            data=data,
            version=version,
            params=params,
        )

        return predictions.to_dict('records')
