from http import HTTPStatus

from flask import request
from flask_restx import Resource, abort
from sqlalchemy.exc import NoResultFound


from mindsdb.api.http.namespaces.configs.projects import ns_conf
from mindsdb.api.executor.controllers.session_controller import SessionController
from mindsdb.metrics.metrics import api_endpoint_metrics


@ns_conf.route('/<project_name>/views')
class ViewsList(Resource):
    @ns_conf.doc('list_views')
    @api_endpoint_metrics('GET', '/views')
    def get(self, project_name):
        '''List all views'''
        session = SessionController()
        try:
            project = session.database_controller.get_project(project_name)
        except NoResultFound:
            abort(HTTPStatus.NOT_FOUND, f'Project name {project_name} does not exist')

        all_views = project.get_views()
        all_view_objs = []
        # Only want to return relevant fields to the user.
        for view in all_views:
            all_view_objs.append({
                'id': view['metadata']['id'],
                'name': view['name'],
                'query': view['query']
            })
        return all_view_objs

    @ns_conf.doc('create_view')
    @api_endpoint_metrics('POST', '/views')
    def post(self, project_name):
        '''Create a new view'''
        if 'view' not in request.json:
            abort(HTTPStatus.BAD_REQUEST, 'Must provide "view" parameter in POST body')
        session = SessionController()
        view_obj = request.json['view']
        if 'name' not in view_obj:
            abort(HTTPStatus.BAD_REQUEST, 'Missing "name" field for view')
        if 'query' not in view_obj:
            abort(HTTPStatus.BAD_REQUEST, 'Missing "query" field for view')
        name = view_obj['name']
        query = view_obj['query']

        try:
            project = session.database_controller.get_project(project_name)
        except NoResultFound:
            abort(HTTPStatus.NOT_FOUND, f'Project name {project_name} does not exist')

        if project.get_view(name) is not None:
            abort(HTTPStatus.CONFLICT, f'View with name {name} already exists.')

        project.create_view(name, query)
        created_view = project.get_view(name)
        # Only want to return relevant fields to the user.
        return {
            'id': created_view['metadata']['id'],
            'name': created_view['name'],
            'query': created_view['query']
        }, HTTPStatus.CREATED


@ns_conf.route('/<project_name>/views/<view_name>')
@ns_conf.param('project_name', 'Name of the project')
@ns_conf.param('view_name', 'Name of the view')
class ViewResource(Resource):
    @ns_conf.doc('get_view')
    @api_endpoint_metrics('GET', '/views/view')
    def get(self, project_name, view_name):
        '''Get a view by name'''
        session = SessionController()
        try:
            project = session.database_controller.get_project(project_name)
        except NoResultFound:
            abort(HTTPStatus.NOT_FOUND, f'Project name {project_name} does not exist')

        view = project.get_view(view_name)
        if view is None:
            abort(HTTPStatus.NOT_FOUND, f'View with name {view_name} does not exist')

        # Only want to return relevant fields to the user.
        return {
            'id': view['metadata']['id'],
            'name': view['name'],
            'query': view['query']
        }

    @ns_conf.doc('update_view')
    @api_endpoint_metrics('PUT', '/views/view')
    def put(self, project_name, view_name):
        '''Updates or creates a view'''
        if 'view' not in request.json:
            abort(HTTPStatus.BAD_REQUEST, 'Must provide "view" parameter in PUT body')
        request_view = request.json['view']
        session = SessionController()
        try:
            project = session.database_controller.get_project(project_name)
        except NoResultFound:
            abort(HTTPStatus.NOT_FOUND, f'Project name {project_name} does not exist')

        existing_view = project.get_view(view_name)
        if existing_view is None:
            # Create
            if 'query' not in request_view:
                abort(HTTPStatus.BAD_REQUEST, 'Missing "query" field for new view')
            project.create_view(view_name, request_view['query'])
            created_view = project.get_view(view_name)
            # Only want to return relevant fields to the user.
            return {
                'id': created_view['metadata']['id'],
                'name': created_view['name'],
                'query': created_view['query']
            }, HTTPStatus.CREATED

        new_query = existing_view['query']
        if 'query' in request_view:
            new_query = request_view['query']
            project.update_view(view_name, new_query)

        existing_view = project.get_view(view_name)
        # Only want to return relevant fields to the user.
        return {
            'id': existing_view['metadata']['id'],
            'name': existing_view['name'],
            'query': existing_view['query']
        }

    @ns_conf.doc('delete_view')
    @api_endpoint_metrics('DELETE', '/views/view')
    def delete(self, project_name, view_name):
        '''Deletes a view by name'''
        session = SessionController()
        try:
            project = session.database_controller.get_project(project_name)
        except NoResultFound:
            abort(HTTPStatus.NOT_FOUND, f'Project name {project_name} does not exist')

        if project.get_view(view_name) is None:
            abort(HTTPStatus.NOT_FOUND, f'View with name {view_name} does not exist')

        project.delete_view(view_name)
        return '', HTTPStatus.NO_CONTENT
