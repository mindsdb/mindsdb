from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers
from mindsdb_sql.parser.ast import Delete, Identifier, BinaryOperation, Constant
from mindsdb_sql.parser.dialects.mindsdb import DropPredictor

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
        if table not in ('models_versions', 'models'):
            raise Exception("Only REMOVE from 'models' collection is supported at this time")

        if len(query['deletes']) != 1:
            raise Exception("Should be only one argument in REMOVE operation")

        obj_name, obj_id = None, None
        project_name = request_env['database']

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

        if obj_name is None and (table == 'models' or table == 'models_versions'):
            model_id = obj_id >> 20
            if obj_name is None:
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

        # delete model
        if table == 'models':
            ast_query = DropPredictor(Identifier(parts=[project_name, obj_name]))
            run_sql_command(mindsdb_env, ast_query)

        # delete model version
        elif table == 'models_versions':
            if version is None:
                if obj_id is None:
                    raise Exception("Can't find object version")

                version = obj_id & (2**20 - 1)

            ast_query = Delete(table=Identifier(parts=[project_name, 'models_versions']),
                               where=BinaryOperation(op='and', args=[
                                   BinaryOperation(op='=', args=[Identifier('name'), Constant(obj_name)]),
                                   BinaryOperation(op='=', args=[Identifier('version'), Constant(version)])
                               ]))
            run_sql_command(mindsdb_env, ast_query)

        return {
            'n': 1,
            'ok': 1
        }


responder = Responce()
