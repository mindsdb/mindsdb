from collections import defaultdict

from flask import current_app as ca
from flask_restx import Resource

from mindsdb.api.http.utils import http_error
from mindsdb.api.http.namespaces.configs.tree import ns_conf


@ns_conf.route('/')
class GetRoot(Resource):
    @ns_conf.doc('get_tree_root')
    def get(self):
        databases = ca.database_controller.get_list()
        result = [{
            'name': x['name'],
            'class': 'db',
            'type': x['type'],
            'engine': x['engine'],
            'deletable': x['type'] != 'system'
        } for x in databases]
        return result


@ns_conf.route('/<db_name>')
@ns_conf.param('db_name', "Name of the database")
class GetLeaf(Resource):
    @ns_conf.doc('get_tree_leaf')
    def get(self, db_name):
        databases = ca.database_controller.get_dict()
        if db_name not in databases:
            return http_error(
                400,
                "Error",
                f"There is no element with name '{db_name}'"
            )
        db = databases[db_name]
        if db['type'] == 'project':
            project = ca.database_controller.get_project(db_name)
            tables = project.get_tables()
            tables = [{
                'name': key,
                'schema': None,
                'class': 'table',
                'type': val['type'],
                'engine': val.get('engine'),
                'deletable': val.get('deletable')
            } for key, val in tables.items()]
        elif db['type'] == 'data':
            handler = ca.integration_controller.get_handler(db_name)
            response = handler.get_tables()
            if response.type != 'table':
                return []
            table_types = {
                'BASE TABLE': 'table',
                'VIEW': 'view'
            }
            tables = response.data_frame.to_dict(orient='records')

            schemas = defaultdict(list)

            for table_meta in tables:
                table_meta = {key.lower(): val for key, val in table_meta.items()}
                schama = table_meta.get('table_schema')
                schemas[schama].append({
                    'name': table_meta['table_name'],
                    'class': 'table',
                    'type': table_types.get(table_meta.get('table_type')),
                    'engine': None,
                    'deletable': False
                })
            if len(schemas) == 1 and list(schemas.keys())[0] is None:
                tables = schemas[None]
            else:
                tables = [{
                    'name': key,
                    'class': 'schema',
                    'deletable': False,
                    'children': val
                } for key, val in schemas.items()]
        elif db['type'] == 'system':
            # TODO
            tables = []
        return tables
