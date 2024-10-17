from http import HTTPStatus

from flask_restx import Resource
from sqlalchemy.exc import NoResultFound

from mindsdb.metrics.metrics import api_endpoint_metrics
from mindsdb.api.http.utils import http_error
from mindsdb.api.http.namespaces.configs.projects import ns_conf
from mindsdb.api.executor.controllers.session_controller import SessionController
from mindsdb.utilities.exception import EntityNotExistsError


@ns_conf.route('/')
class ProjectsList(Resource):
    @ns_conf.doc('list_projects')
    @api_endpoint_metrics('GET', '/projects')
    def get(self):
        ''' List all projects '''
        session = SessionController()

        projects = [
            {'name': i}
            for i in session.datahub.get_projects_names()
        ]
        return projects


@ns_conf.route('/<project_name>')
class ProjectsGet(Resource):
    @ns_conf.doc('get_project')
    @api_endpoint_metrics('GET', '/projects/project')
    def get(self, project_name):
        '''Gets a project by name'''
        session = SessionController()

        try:
            project = session.database_controller.get_project(project_name)
        except (NoResultFound, EntityNotExistsError):
            return http_error(
                HTTPStatus.NOT_FOUND, 'Project not exists',
                f'Project name {project_name} does not exist'
            )

        return {
            'name': project.name
        }
