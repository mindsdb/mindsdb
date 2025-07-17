import uuid

from bson.int64 import Int64
from mindsdb_sql_parser.ast import Show, Identifier

from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers
from mindsdb.api.mongo.classes.query_sql import run_sql_command


class Responce(Responder):
    when = {'listCollections': helpers.is_true}

    def result(self, query, request_env, mindsdb_env, session):
        database = request_env['database']
        ast_query = Show(
            category='tables',
            from_table=Identifier(parts=[database])
        )
        data = run_sql_command(request_env, ast_query)

        tables = []
        for row in data:
            name = list(row.values())[0]  # first value
            tables.append({
                'name': name,
                'type': 'collection',
                'options': {},
                'info': {
                    'readOnly': False,
                    'uuid': uuid.uuid3(uuid.NAMESPACE_DNS, name).bytes,
                },
                'idIndex': {
                    'v': 2,
                    'key': {
                        '_id': 1
                    },
                    'name': '_id_',
                    'ns': f'{database}.{name}'
                }
            })

        cursor = {
            'id': Int64(0),  # should we save id somewhere?
            'ns': f'{database}.$cmd.listCollections',
            'firstBatch': tables
        }

        return {
            'cursor': cursor,
            'ok': 1,
        }


responder = Responce()
