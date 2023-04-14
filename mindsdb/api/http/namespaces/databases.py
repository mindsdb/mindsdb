from http import HTTPStatus
from sqlalchemy.exc import NoResultFound

from flask import request
from flask_restx import Resource, abort

from mindsdb.api.http.namespaces.configs.databases import ns_conf
from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController


@ns_conf.route('/')
class DatabasesResource(Resource):
    @ns_conf.doc('list_databases')
    def get(self):
        '''List all databases'''
        session = SessionController()
        return session.database_controller.get_list()

    @ns_conf.doc('create_database')
    def post(self):
        '''Create a database'''
        if 'database' not in request.json:
            abort(HTTPStatus.BAD_REQUEST, 'Must provide "database" parameter in POST body')
        session = SessionController()
        database = request.json['database']
        parameters = {}
        if 'name' not in database:
            abort(HTTPStatus.BAD_REQUEST, 'Missing "name" field for database')
        if 'engine' not in database:
            abort(HTTPStatus.BAD_REQUEST, 'Missing "engine" field for database. If you want to create a project instead, use the /api/projects endpoint.')
        if 'parameters' in database:
            parameters = database['parameters']
        name = database['name']

        if session.database_controller.exists(name):
            abort(HTTPStatus.CONFLICT, f'Database with name {name} already exists.')
        new_integration_id = session.integration_controller.add(name, database['engine'], parameters)
        new_integration = session.database_controller.get_integration(new_integration_id)
        return new_integration, HTTPStatus.CREATED


@ns_conf.route('/<database_name>')
class DatabaseResource(Resource):
    @ns_conf.doc('get_database')
    def get(self, database_name):
        '''Gets a database by name'''
        session = SessionController()
        try:
            project = session.database_controller.get_project(database_name)
            return {
                'name': database_name,
                'type': 'project',
                'id': project.id,
                'engine': None
            }
        except NoResultFound:
            integration = session.integration_controller.get(database_name)
            if integration is None:
                abort(HTTPStatus.NOT_FOUND, f'Database with name {database_name} does not exist.')
            return integration

    @ns_conf.doc('update_database')
    def put(self, database_name):
        '''Updates or creates a database'''
        if 'database' not in request.json:
            abort(HTTPStatus.BAD_REQUEST, 'Must provide "database" parameter in POST body')

        session = SessionController()
        parameters = {}
        database = request.json['database']
        if 'parameters' in database:
            parameters = database['parameters']
        if not session.database_controller.exists(database_name):
            # Create.
            if 'engine' not in database:
                abort(HTTPStatus.BAD_REQUEST, 'Missing "engine" field for new database. If you want to create a project instead, use the POST /api/projects endpoint.')
            new_integration_id = session.integration_controller.add(database_name, database['engine'], parameters)
            new_integration = session.database_controller.get_integration(new_integration_id)
            return new_integration, HTTPStatus.CREATED

        session.integration_controller.modify(database_name, parameters)
        return session.integration_controller.get(database_name)

    @ns_conf.doc('delete_database')
    def delete(self, database_name):
        '''Deletes a database by name'''
        session = SessionController()
        if not session.database_controller.exists(database_name):
            abort(HTTPStatus.NOT_FOUND, f'Database with name {database_name} does not exist.')
        try:
            session.database_controller.delete(database_name)
        except Exception as e:
            abort(
                HTTPStatus.BAD_REQUEST,
                f'Cannot delete database {database_name}. '
                + 'This is most likely a system database, a permanent integration, or an ML engine with active models. '
                + f'Full error: {e}. '
                + 'Please check the name and try again.')
        return '', HTTPStatus.NO_CONTENT
