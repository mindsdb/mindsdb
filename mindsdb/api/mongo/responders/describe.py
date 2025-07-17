from mindsdb_sql_parser.ast import Describe, Identifier

from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers
from mindsdb.api.mongo.classes.query_sql import run_sql_command


class Responce(Responder):
    when = {'describe': helpers.is_true}

    def result(self, query, request_env, mindsdb_env, session):
        db = query['$db']
        model = query['describe']

        ast_query = Describe(Identifier(model))

        data = run_sql_command(request_env, ast_query)
        res = {'data': data, 'ns': f"{db}.{model}"}

        return res


responder = Responce()
