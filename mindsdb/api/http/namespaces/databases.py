from http import HTTPStatus
from sqlalchemy.exc import NoResultFound
from typing import Dict

from flask import request
from flask_restx import Resource, abort

from mindsdb.api.http.namespaces.configs.databases import ns_conf
from mindsdb.api.mysql.mysql_proxy.classes.fake_mysql_proxy import FakeMysqlProxy
from mindsdb.api.executor.controllers.session_controller import SessionController
from mindsdb.api.executor.datahub.classes.tables_row import TablesRow
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.metrics.metrics import api_endpoint_metrics
from mindsdb_sql import parse_sql, ParsingException
from mindsdb_sql.parser.ast import CreateTable, DropTables


@ns_conf.route('/')
class DatabasesResource(Resource):
    @ns_conf.doc('list_databases')
    @api_endpoint_metrics('GET', '/databases')
    def get(self):
        '''List all databases'''
        session = SessionController()
        return session.database_controller.get_list()

    @ns_conf.doc('create_database')
    @api_endpoint_metrics('POST', '/databases')
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
    @api_endpoint_metrics('GET', '/databases/database')
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
    @api_endpoint_metrics('PUT', '/databases/database')
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
    @api_endpoint_metrics('DELETE', '/databases/database')
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


def _tables_row_to_obj(table_row: TablesRow) -> Dict:
    type = table_row.TABLE_TYPE.lower()
    if table_row.TABLE_TYPE == 'BASE TABLE':
        type = 'data'
    return {
        'name': table_row.TABLE_NAME,
        'type': type
    }


@ns_conf.route('/<database_name>/tables')
class TablesList(Resource):
    @ns_conf.doc('list_tables')
    @api_endpoint_metrics('GET', '/databases/database/tables')
    def get(self, database_name):
        '''Get all tables in a database'''
        session = SessionController()
        datanode = session.datahub.get(database_name)
        all_tables = datanode.get_tables()
        table_objs = [_tables_row_to_obj(t) for t in all_tables]
        return table_objs

    @ns_conf.doc('create_table')
    @api_endpoint_metrics('POST', '/databases/database/tables')
    def post(self, database_name):
        '''Creates a table in a database'''
        if 'table' not in request.json:
            abort(HTTPStatus.BAD_REQUEST, 'Must provide "table" parameter in POST body')
        table = request.json['table']
        if 'name' not in table:
            abort(HTTPStatus.BAD_REQUEST, 'Missing "name" field for table')
        if 'select' not in table:
            abort(HTTPStatus.BAD_REQUEST, 'Missing "select" field for table')
        table_name = table['name']
        select_query = table['select']
        replace = False
        if 'replace' in table:
            replace = table['replace']

        session = SessionController()
        try:
            session.database_controller.get_project(database_name)
            error_message = f'Database {database_name} is a project. ' \
                + f'If you want to create a model or view, use the projects/{database_name}/models/{table_name} or ' \
                + f'projects/{database_name}/views/{table_name} endpoints instead.'
            abort(HTTPStatus.BAD_REQUEST, error_message)
        except NoResultFound:
            # Only support creating tables from integrations.
            pass

        datanode = session.datahub.get(database_name)
        if datanode is None:
            abort(HTTPStatus.NOT_FOUND, f'Database with name {database_name} does not exist')
        all_tables = datanode.get_tables()
        for t in all_tables:
            if t.TABLE_NAME == table_name and not replace:
                abort(HTTPStatus.CONFLICT, f'Table with name {table_name} already exists')

        try:
            select_ast = parse_sql(select_query, dialect='mindsdb')
        except ParsingException:
            abort(HTTPStatus.BAD_REQUEST, f'Could not parse select statement {select_query}')

        create_ast = CreateTable(
            f'{database_name}.{table_name}',
            from_select=select_ast,
            is_replace=replace
        )

        mysql_proxy = FakeMysqlProxy()

        try:
            mysql_proxy.process_query(create_ast.get_string())
        except Exception as e:
            abort(HTTPStatus.BAD_REQUEST, e)

        all_tables = datanode.get_tables()
        try:
            matching_table = next(t for t in all_tables if t.TABLE_NAME == table_name)
            return _tables_row_to_obj(matching_table), HTTPStatus.CREATED
        except StopIteration:
            abort(HTTPStatus.INTERNAL_SERVER_ERROR, f'Table with name {table_name} could not be created')


@ns_conf.route('/<database_name>/tables/<table_name>')
@ns_conf.param('database_name', 'Name of the database')
@ns_conf.param('table_name', 'Name of the table')
class TableResource(Resource):
    @ns_conf.doc('get_table')
    @api_endpoint_metrics('GET', '/databases/database/tables/table')
    def get(self, database_name, table_name):
        session = SessionController()
        datanode = session.datahub.get(database_name)
        all_tables = datanode.get_tables()
        try:
            matching_table = next(t for t in all_tables if t.TABLE_NAME == table_name)
            return _tables_row_to_obj(matching_table)
        except StopIteration:
            abort(HTTPStatus.NOT_FOUND, f'Table with name {table_name} not found')

    @ns_conf.doc('drop_table')
    @api_endpoint_metrics('DELETE', '/databases/database/tables/table')
    def delete(self, database_name, table_name):
        session = SessionController()
        try:
            session.database_controller.get_project(database_name)
            error_message = f'Database {database_name} is a project. ' \
                + f'If you want to delete a model or view, use the projects/{database_name}/models/{table_name} or ' \
                + f'projects/{database_name}/views/{table_name} endpoints instead.'
            abort(HTTPStatus.BAD_REQUEST, error_message)
        except NoResultFound:
            # Only support dropping tables from integrations.
            pass

        datanode = session.datahub.get(database_name)
        if datanode is None:
            abort(HTTPStatus.NOT_FOUND, f'Database with name {database_name} not found')
        all_tables = datanode.get_tables()
        try:
            next(t for t in all_tables if t.TABLE_NAME == table_name)
        except StopIteration:
            abort(HTTPStatus.NOT_FOUND, f'Table with name {table_name} not found')

        drop_ast = DropTables(
            tables=[table_name],
            if_exists=True
        )

        try:
            integration_handler = session.integration_controller.get_data_handler(database_name)
        except Exception:
            abort(HTTPStatus.INTERNAL_SERVER_ERROR, f'Could not get database handler for {database_name}')
        try:
            result = integration_handler.query(drop_ast)
        except NotImplementedError:
            abort(HTTPStatus.BAD_REQUEST, f'Database {database_name} does not support dropping tables.')
        if result.type == RESPONSE_TYPE.ERROR:
            abort(HTTPStatus.BAD_REQUEST, result.error_message)
        return '', HTTPStatus.NO_CONTENT
