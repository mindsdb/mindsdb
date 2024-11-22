from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers
from mindsdb_sql_parser.ast import Identifier
from mindsdb_sql_parser.ast.mindsdb import DropPredictor, DropJob, DropMLEngine
from mindsdb.interfaces.jobs.jobs_controller import JobsController

from mindsdb.api.mongo.classes.query_sql import run_sql_command


class Responce(Responder):
    when = {'delete': helpers.is_true}

    def result(self, query, request_env, mindsdb_env, session):
        try:
            res = self._result(query, request_env, mindsdb_env)
        except Exception as e:
            res = {
                'n': 0,
                'writeErrors': [{
                    'index': 0,
                    'code': 0,
                    'errmsg': str(e)
                }],
                'ok': 1
            }
        return res

    def _result(self, query, request_env, mindsdb_env):
        table = query['delete']
        if table == 'predictors':
            table = 'models'

        project_name = request_env['database']

        allowed_tables = ('models', 'jobs', 'ml_engines')
        if table not in allowed_tables:
            raise Exception(f"Only removing from this collections is supported: {', '.join(allowed_tables)}")

        if len(query['deletes']) != 1:
            raise Exception("Should be only one argument in REMOVE operation")

        obj_name, obj_id = None, None

        delete_filter = query['deletes'][0]['q']
        if '_id' in delete_filter:
            # get name of object
            obj_id = helpers.objectid_to_int(delete_filter['_id'])

        if 'name' in delete_filter:
            obj_name = delete_filter['name']

        version = None
        if 'version' in delete_filter:
            version = delete_filter['version']

        if obj_name is None and obj_id is None:
            raise Exception("Can't find object to delete, use filter by name or _id")

        if obj_name is None:
            if table == 'models':
                model_id = obj_id >> 20
                version = obj_id & (2 ** 20 - 1)

                models = mindsdb_env['model_controller'].get_models(
                    ml_handler_name=None,
                    project_name=project_name
                )
                for model in models:
                    if model['id'] == model_id:
                        obj_name = model['name']
                        break
                if obj_name is None:
                    raise Exception("Can't find model by _id")
            elif table == 'jobs':
                jobs_controller = JobsController()
                for job in jobs_controller.get_list(project_name):
                    if job['id'] == obj_id:
                        obj_name = job['name']
                        break

        # delete model
        if table == 'models':
            if version is None:
                ast_query = DropPredictor(Identifier(parts=[project_name, obj_name]))
                run_sql_command(request_env, ast_query)
            else:
                # delete model version
                ast_query = DropPredictor(Identifier(parts=[project_name, obj_name, str(version)]))
                run_sql_command(request_env, ast_query)

        elif table == 'jobs':
            ast_query = DropJob(Identifier(parts=[project_name, obj_name]))
            run_sql_command(request_env, ast_query)

        elif table == 'ml_engines':
            ast_query = DropMLEngine(Identifier(parts=[obj_name]))
            run_sql_command(request_env, ast_query)

        return {
            'n': 1,
            'ok': 1
        }


responder = Responce()
