from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers

from mindsdb_sql.parser.ast import *

from mindsdb.api.mongo.classes.query_sql import run_sql_command


class Responce(Responder):
    when = {'collStats': helpers.is_true}

    def result(self, query, request_env, mindsdb_env, session):
        db = query['$db']
        collection = query['collStats']

        if db == 'mindsdb' and collection == 'predictors':
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

            res['count'] = len(mindsdb_env['model_interface'].get_models())
        else:
            scale = query.get('scale')

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
