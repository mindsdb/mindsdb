from mindsdb_sql.parser.ast import Describe, Identifier

from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers
from mindsdb.api.mongo.classes.query_sql import run_sql_command


class Responce(Responder):
    when = {'collStats': helpers.is_true}

    def result(self, query, request_env, mindsdb_env, session):
        db = query['$db']
        collection = query['collStats']

        scale = query.get('scale')

        if db != 'mindsdb' or collection == 'predictors' or scale is None:
            # old behavior
            # NOTE real answer is huge, i removed most data from it.
            res = {
                'ns': "db.collection",
                'size': 1,
                'count': 0,
                'avgObjSize': 1,
                'storageSize': 16384,
                'capped': False,
                'wiredTiger': {
                },
                'nindexes': 1,
                'indexDetails': {
                },
                'totalIndexSize': 16384,
                'indexSizes': {
                    '_id_': 16384
                },
                'ok': 1
            }

            res['ns'] = f"{db}.{collection}"
            if db == 'mindsdb' and collection == 'predictors':
                res['count'] = len(mindsdb_env['model_controller'].get_models())
        else:

            ident_parts = [collection]
            if scale is not None:
                ident_parts.append(scale)

            ast_query = Describe(Identifier(
                parts=ident_parts
            ))

            data = run_sql_command(mindsdb_env, ast_query)
            res = {
                'data': data
            }

        res['ns'] = f"{db}.{collection}"

        return res


responder = Responce()
