from bson.int64 import Int64

from mindsdb_sql.parser.ast import *
import mindsdb.api.mongo.functions as helpers
from mindsdb.api.mongo.classes import Responder
from mindsdb.integrations.handlers.mongodb_handler.utils.mongodb_ast import MongoToAst

from mindsdb.api.mongo.classes.query_sql import run_sql_command


class Responce(Responder):
    when = {'find': helpers.is_true}

    def result(self, query, request_env, mindsdb_env, session):
        database = request_env['database']

        # system queries
        if query['find'] == 'system.version':
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
            filter = query['filter']['query']
            table_select = mongoToAst.find(
                collection=collection,
                filter=filter,
            )
            table_select.parentheses = True

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

        data = run_sql_command(mindsdb_env, ast_query)

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
