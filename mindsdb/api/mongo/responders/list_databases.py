from mindsdb_sql.parser.ast import *
from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers

from mindsdb.api.mongo.classes.query_sql import run_sql_command


class Responce(Responder):
    when = {'listDatabases': helpers.is_true}

    def result(self, query, request_env, mindsdb_env, session):

        ast_query = Show(category='databases')
        data = run_sql_command(mindsdb_env, ast_query)

        databases = [
            {
                'name': 'admin',
                'sizeOnDisk': 1 << 16,
                'empty': False
            }
        ]

        for row in data:
            databases.append({
                'name': list(row.values())[0],  # first value
                'sizeOnDisk': 1 << 16,
                'empty': False
            })

        return {
            'databases': databases,
            'ok': 1,
        }


responder = Responce()
