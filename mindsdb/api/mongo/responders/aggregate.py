from bson.int64 import Int64

from mindsdb_sql_parser.ast import Identifier, Insert, CreateTable

from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers
from mindsdb.api.mongo.responders.find import find_to_ast
from mindsdb.api.mongo.classes.query_sql import run_sql_command
from mindsdb.utilities.config import config


def aggregate_to_ast(query, database):
    collection = query['aggregate']

    save_table = None
    is_append = False
    first_step = query['pipeline'][0]
    if '$match' in first_step:
        # convert to find
        find_query = {
            'find': collection,
            'filter': first_step['$match'],
        }
        for step in query['pipeline'][1:]:
            if '$project' in step:
                find_query['projection'] = step['$project']
            if '$sort' in step:
                find_query['sort'] = step['$sort']
            if '$skip' in step:
                find_query['skip'] = step['$skip']
            if '$limit' in step:
                find_query['limit'] = step['$limit']

            if '$out' in step:
                target = step['$out']
                if isinstance(target, str):
                    save_table = Identifier(target)
                else:
                    save_table = Identifier(parts=[target['db'], target['coll']])
                    if target.get('append'):
                        is_append = True

            # TODO implement group
        ast_query = find_to_ast(find_query, database)
        if save_table is not None:
            if is_append:
                ast_query = Insert(save_table, from_select=ast_query)
            else:
                ast_query = CreateTable(save_table, from_select=ast_query, is_replace=True)

    else:
        raise NotImplementedError

    return ast_query


class Responce(Responder):
    when = {'aggregate': helpers.is_true}

    def result(self, query, request_env, mindsdb_env, session):
        db = query['$db']
        collection = query['aggregate']

        first_step = query['pipeline'][0]
        if '$match' in first_step:
            ast_query = aggregate_to_ast(query, request_env.get('database', config.get('default_project')))

            data = run_sql_command(request_env, ast_query)

        elif '$collStats' in first_step:
            raise ValueError(
                "To describe model use:"
                " db.runCommand({describe: 'model_name.attribute'})"
            )

        else:
            raise NotImplementedError

        cursor = {
            'id': Int64(0),
            'ns': f"{db}.$cmd.{collection}",
            'firstBatch': data
        }
        return {
            'cursor': cursor,
            'ok': 1
        }


responder = Responce()
