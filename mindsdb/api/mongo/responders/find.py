from bson.int64 import Int64

from mindsdb_sql_parser.ast import Join, Select, Identifier, Describe, Show, Constant
import mindsdb.api.mongo.functions as helpers
from mindsdb.api.mongo.classes import Responder
from mindsdb.api.mongo.utilities.mongodb_ast import MongoToAst
from mindsdb.interfaces.jobs.jobs_controller import JobsController

from mindsdb.api.mongo.classes.query_sql import run_sql_command


def find_to_ast(query, database):
    mongoToAst = MongoToAst()

    collection = [database, query['find']]

    if not query.get('singleBatch') and 'collection' in query.get('filter'):
        # JOIN mode

        # upper query
        ast_query = mongoToAst.find(
            collection=collection,
            projection=query.get('projection'),
            sort=query.get('sort'),
            limit=query.get('limit'),
            skip=query.get('skip'),
        )

        # table_query
        collection = query['filter']['collection']
        filter = query['filter'].get('query', {})
        table_select = mongoToAst.find(
            collection=collection,
            filter=filter,
        )
        table_select.parentheses = True
        table_select.alias = Identifier(query['filter']['collection'])
        table_select.alias.parts = [table_select.alias.parts[-1]]
        if 'limit' in query:
            table_select.limit = Constant(query['limit'])

        modifiers = query['filter'].get('modifiers')
        if modifiers is not None and hasattr(ast_query, 'modifiers'):
            for modifier in modifiers:
                table_select.modifiers.append(modifier)

        # convert to join
        right_table = ast_query.from_table

        ast_join = Join(
            left=table_select,
            right=right_table,
            join_type='join'
        )
        ast_query.from_table = ast_join

    else:
        # is single table
        ast_query = mongoToAst.find(
            collection=collection,
            filter=query.get('filter'),
            projection=query.get('projection'),
            sort=query.get('sort'),
            limit=query.get('limit'),
            skip=query.get('skip'),
        )
        modifiers = query['filter'].get('modifiers')
        if modifiers is not None and hasattr(ast_query, 'modifiers'):
            for modifier in modifiers:
                ast_query.modifiers.append(modifier)
    return ast_query


class Responce(Responder):
    when = {'find': helpers.is_true}

    def result(self, query, request_env, mindsdb_env, session):
        database = request_env['database']
        project_name = request_env['database']

        if database == 'config':
            # return nothing
            return {
                'cursor': {
                    'id': Int64(0),
                    'ns': f"{database}.$cmd.{query['find']}",
                    'firstBatch': []
                },
                'ok': 1
            }

        # system queries
        elif query['find'] == 'system.version':
            # For studio3t
            data = [{
                "_id": "featureCompatibilityVersion",
                "version": "3.6"
            }]
            cursor = {
                'id': Int64(0),
                'ns': f"system.version.$cmd.{query['find']}",
                'firstBatch': data
            }
            return {
                'cursor': cursor,
                'ok': 1
            }

        elif query['find'] == 'ml_engines':
            ast_query = Show('ml_engines')

        else:
            ast_query = find_to_ast(query, database)

        # add _id for objects
        table_name = None
        obj_idx = {}
        if isinstance(ast_query, Select) and ast_query.from_table is not None:
            if isinstance(ast_query.from_table, Identifier):
                table_name = ast_query.from_table.parts[-1].lower()

                if table_name == 'models':

                    models = mindsdb_env['model_controller'].get_models(
                        ml_handler_name=None,
                        project_name=project_name
                    )

                    for model in models:
                        obj_idx[model['name']] = model['id']

                    # is select from model without where
                    if table_name in obj_idx and ast_query.where is None:
                        # replace query to describe model
                        ast_query = Describe(ast_query.from_table)
                elif table_name == 'jobs':
                    jobs_controller = JobsController()
                    for job in jobs_controller.get_list(project_name):
                        obj_idx[job['name']] = job['id']

        data = run_sql_command(request_env, ast_query)

        if table_name == 'models':
            # for models and models_versions _id is:
            #   - first 20 bytes is version
            #   - next bytes is model id

            for row in data:
                model_id = obj_idx.get(row.get('NAME'))
                if model_id is not None:
                    obj_id = model_id << 20

                    obj_id += row.get('VERSION', 0)

                    row['_id'] = helpers.int_to_objectid(obj_id)
        elif table_name == 'jobs':
            for row in data:
                obj_id = obj_idx.get(row.get('NAME'))
                if obj_id is not None:
                    row['_id'] = helpers.int_to_objectid(obj_id)

        db = mindsdb_env['config']['api']['mongodb']['database']

        cursor = {
            'id': Int64(0),
            'ns': f"{db}.$cmd.{query['find']}",
            'firstBatch': data
        }
        return {
            'cursor': cursor,
            'ok': 1
        }


responder = Responce()
