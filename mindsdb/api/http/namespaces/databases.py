import time
import shutil
import tempfile
from http import HTTPStatus
from typing import Dict
from pathlib import Path

from flask import request
from flask_restx import Resource

from mindsdb.api.http.utils import http_error
from mindsdb.api.http.namespaces.configs.databases import ns_conf
from mindsdb.api.mysql.mysql_proxy.classes.fake_mysql_proxy import FakeMysqlProxy
from mindsdb.api.executor.controllers.session_controller import SessionController
from mindsdb.api.executor.datahub.classes.tables_row import TablesRow
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.metrics.metrics import api_endpoint_metrics
from mindsdb_sql_parser import parse_sql, ParsingException
from mindsdb_sql_parser.ast import CreateTable, DropTables
from mindsdb.utilities.exception import EntityNotExistsError
from mindsdb.integrations.libs.response import HandlerStatusResponse


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
            return http_error(
                HTTPStatus.BAD_REQUEST, 'Wrong argument',
                'Must provide "database" parameter in POST body'
            )
        check_connection = request.json.get('check_connection', False)
        session = SessionController()
        database = request.json['database']
        parameters = {}
        if 'name' not in database:
            return http_error(
                HTTPStatus.BAD_REQUEST, 'Wrong argument',
                'Missing "name" field for database'
            )
        if 'engine' not in database:
            return http_error(
                HTTPStatus.BAD_REQUEST, 'Wrong argument',
                'Missing "engine" field for database. If you want to create a project instead, use the /api/projects endpoint.'
            )
        if 'parameters' in database:
            parameters = database['parameters']
        name = database['name']

        if session.database_controller.exists(name):
            return http_error(
                HTTPStatus.CONFLICT, 'Name conflict',
                f'Database with name {name} already exists.'
            )

        storage = None
        if check_connection:
            try:
                handler = session.integration_controller.create_tmp_handler(name, database['engine'], parameters)
                status = handler.check_connection()
            except ImportError as import_error:
                status = HandlerStatusResponse(success=False, error_message=str(import_error))

            if status.success is not True:
                if hasattr(status, 'redirect_url') and isinstance(status, str):
                    return {
                        "status": "redirect_required",
                        "redirect_url": status.redirect_url,
                        "detail": status.error_message
                    }, HTTPStatus.OK
                return {
                    "status": "connection_error",
                    "detail": status.error_message
                }, HTTPStatus.OK

            if status.copy_storage:
                storage = handler.handler_storage.export_files()

        new_integration_id = session.integration_controller.add(name, database['engine'], parameters)

        if storage:
            handler = session.integration_controller.get_data_handler(name, connect=False)
            handler.handler_storage.import_files(storage)

        new_integration = session.database_controller.get_integration(new_integration_id)
        return new_integration, HTTPStatus.CREATED


@ns_conf.route('/status')
class DatabasesStatusResource(Resource):
    @ns_conf.doc('check_database_connection_status')
    @api_endpoint_metrics('POST', '/databases/status')
    def post(self):
        '''Check the connection parameters for a database'''
        data = {}
        if request.content_type == 'application/json':
            data.update(request.json or {})
        elif request.content_type.startswith('multipart/form-data'):
            data.update(request.form or {})

        if 'engine' not in data:
            return http_error(
                HTTPStatus.BAD_REQUEST, 'Wrong argument',
                'Missing "engine" field for database'
            )

        engine = data['engine']
        parameters = data
        del parameters['engine']

        files = request.files
        temp_dir = None
        if files is not None and len(files) > 0:
            temp_dir = tempfile.mkdtemp(prefix='integration_files_')
            for key, file in files.items():
                temp_dir_path = Path(temp_dir)
                file_name = Path(file.filename)
                file_path = temp_dir_path.joinpath(file_name).resolve()
                if temp_dir_path not in file_path.parents:
                    raise Exception(f'Can not save file at path: {file_path}')
                file.save(file_path)
                parameters[key] = str(file_path)

        session = SessionController()

        try:
            handler = session.integration_controller.create_tmp_handler("test_connection", engine, parameters)
            status = handler.check_connection()
        except ImportError as import_error:
            status = HandlerStatusResponse(success=False, error_message=str(import_error))
        except Exception as unknown_error:
            status = HandlerStatusResponse(success=False, error_message=str(unknown_error))
        finally:
            if temp_dir is not None:
                shutil.rmtree(temp_dir)

        if not status.success:
            if hasattr(status, 'redirect_url') and isinstance(status, str):
                return {
                    "status": "redirect_required",
                    "redirect_url": status.redirect_url,
                    "detail": status.error_message
                }, HTTPStatus.OK
            return {
                "status": "connection_error",
                "detail": status.error_message
            }, HTTPStatus.OK

        return {
            "status": "success",
        }, HTTPStatus.OK


@ns_conf.route('/<database_name>')
class DatabaseResource(Resource):
    @ns_conf.doc('get_database')
    @api_endpoint_metrics('GET', '/databases/database')
    def get(self, database_name):
        '''Gets a database by name'''
        session = SessionController()
        check_connection = request.args.get('check_connection', 'false').lower() in ('1', 'true')
        try:
            project = session.database_controller.get_project(database_name)
            result = {
                'name': database_name,
                'type': 'project',
                'id': project.id,
                'engine': None
            }
            if check_connection:
                result['connection_status'] = {
                    'success': True,
                    'error_message': None
                }
        except (ValueError, EntityNotExistsError):
            integration = session.integration_controller.get(database_name)
            if integration is None:
                return http_error(
                    HTTPStatus.NOT_FOUND, 'Database not found',
                    f'Database with name {database_name} does not exist.'
                )
            result = integration
            if check_connection:
                integration['connection_status'] = {
                    'success': False,
                    'error_message': None
                }
                try:
                    handler = session.integration_controller.get_data_handler(database_name)
                    status = handler.check_connection()
                    integration['connection_status']['success'] = status.success
                    integration['connection_status']['error_message'] = status.error_message
                except Exception as e:
                    integration['connection_status']['success'] = False
                    integration['connection_status']['error_message'] = str(e)

        return result

    @ns_conf.doc('update_database')
    @api_endpoint_metrics('PUT', '/databases/database')
    def put(self, database_name):
        '''Updates or creates a database'''
        if 'database' not in request.json:
            return http_error(
                HTTPStatus.BAD_REQUEST, 'Wrong argument',
                'Must provide "database" parameter in POST body'
            )

        session = SessionController()
        parameters = {}
        database = request.json['database']
        check_connection = request.json.get('check_connection', False)

        if 'parameters' in database:
            parameters = database['parameters']
        if not session.database_controller.exists(database_name):
            # Create.
            if 'engine' not in database:
                return http_error(
                    HTTPStatus.BAD_REQUEST, 'Wrong argument',
                    'Missing "engine" field for new database. '
                    'If you want to create a project instead, use the POST /api/projects endpoint.'
                )
            new_integration_id = session.integration_controller.add(database_name, database['engine'], parameters)
            new_integration = session.database_controller.get_integration(new_integration_id)
            return new_integration, HTTPStatus.CREATED

        if check_connection:
            existing_integration = session.integration_controller.get(database_name)
            temp_name = f'{database_name}_{time.time()}'.replace('.', '')
            try:
                handler = session.integration_controller.create_tmp_handler(
                    temp_name, existing_integration['engine'], parameters
                )
                status = handler.check_connection()
            except ImportError as import_error:
                status = HandlerStatusResponse(success=False, error_message=str(import_error))

            if status.success is not True:
                return http_error(
                    HTTPStatus.BAD_REQUEST, 'Connection error',
                    status.error_message or 'Connection error'
                )

        session.integration_controller.modify(database_name, parameters)
        return session.integration_controller.get(database_name)

    @ns_conf.doc('delete_database')
    @api_endpoint_metrics('DELETE', '/databases/database')
    def delete(self, database_name):
        '''Deletes a database by name'''
        session = SessionController()
        if not session.database_controller.exists(database_name):
            return http_error(
                HTTPStatus.NOT_FOUND, 'Database not found',
                f'Database with name {database_name} does not exist.'
            )
        try:
            session.database_controller.delete(database_name)
        except Exception as e:
            return http_error(
                HTTPStatus.BAD_REQUEST, 'Error',
                f'Cannot delete database {database_name}. '
                + 'This is most likely a system database, a permanent integration, or an ML engine with active models. '
                + f'Full error: {e}. '
                + 'Please check the name and try again.'
            )
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
            return http_error(
                HTTPStatus.BAD_REQUEST, 'Wrong argument',
                'Must provide "table" parameter in POST body'
            )
        table = request.json['table']
        if 'name' not in table:
            return http_error(
                HTTPStatus.BAD_REQUEST, 'Wrong argument',
                'Missing "name" field for table'
            )
        if 'select' not in table:
            return http_error(
                HTTPStatus.BAD_REQUEST, 'Wrong argument',
                'Missing "select" field for table'
            )
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
            return http_error(
                HTTPStatus.BAD_REQUEST, 'Error',
                error_message
            )
        except EntityNotExistsError:
            # Only support creating tables from integrations.
            pass

        datanode = session.datahub.get(database_name)
        if datanode is None:
            return http_error(
                HTTPStatus.NOT_FOUND, 'Database not exists',
                f'Database with name {database_name} does not exist'
            )
        all_tables = datanode.get_tables()
        for t in all_tables:
            if t.TABLE_NAME == table_name and not replace:
                return http_error(
                    HTTPStatus.CONFLICT, 'Name conflict',
                    f'Table with name {table_name} already exists'
                )

        try:
            select_ast = parse_sql(select_query)
        except ParsingException:
            return http_error(
                HTTPStatus.BAD_REQUEST, 'Error',
                f'Could not parse select statement {select_query}'
            )

        create_ast = CreateTable(
            f'{database_name}.{table_name}',
            from_select=select_ast,
            is_replace=replace
        )

        mysql_proxy = FakeMysqlProxy()

        try:
            mysql_proxy.process_query(create_ast.get_string())
        except Exception as e:
            return http_error(
                HTTPStatus.BAD_REQUEST, 'Error',
                str(e)
            )

        all_tables = datanode.get_tables()
        try:
            matching_table = next(t for t in all_tables if t.TABLE_NAME == table_name)
            return _tables_row_to_obj(matching_table), HTTPStatus.CREATED
        except StopIteration:
            return http_error(
                HTTPStatus.INTERNAL_SERVER_ERROR, 'Error',
                f'Table with name {table_name} could not be created'
            )


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
            return http_error(
                HTTPStatus.NOT_FOUND, 'Table not found',
                f'Table with name {table_name} not found'
            )

    @ns_conf.doc('drop_table')
    @api_endpoint_metrics('DELETE', '/databases/database/tables/table')
    def delete(self, database_name, table_name):
        session = SessionController()
        try:
            session.database_controller.get_project(database_name)
            error_message = f'Database {database_name} is a project. ' \
                + f'If you want to delete a model or view, use the projects/{database_name}/models/{table_name} or ' \
                + f'projects/{database_name}/views/{table_name} endpoints instead.'
            return http_error(HTTPStatus.BAD_REQUEST, 'Error', error_message)
        except EntityNotExistsError:
            # Only support dropping tables from integrations.
            pass

        datanode = session.datahub.get(database_name)
        if datanode is None:
            return http_error(
                HTTPStatus.NOT_FOUND, 'Database not found',
                f'Database with name {database_name} not found'
            )
        all_tables = datanode.get_tables()
        try:
            next(t for t in all_tables if t.TABLE_NAME == table_name)
        except StopIteration:
            return http_error(
                HTTPStatus.NOT_FOUND, 'Table not found',
                f'Table with name {table_name} not found'
            )

        drop_ast = DropTables(
            tables=[table_name],
            if_exists=True
        )

        try:
            integration_handler = session.integration_controller.get_data_handler(database_name)
        except Exception:
            return http_error(
                HTTPStatus.INTERNAL_SERVER_ERROR, 'Error',
                f'Could not get database handler for {database_name}'
            )
        try:
            result = integration_handler.query(drop_ast)
        except NotImplementedError:
            return http_error(
                HTTPStatus.BAD_REQUEST, 'Error',
                f'Database {database_name} does not support dropping tables.'
            )
        if result.type == RESPONSE_TYPE.ERROR:
            return http_error(HTTPStatus.BAD_REQUEST, 'Error', result.error_message)
        return '', HTTPStatus.NO_CONTENT
