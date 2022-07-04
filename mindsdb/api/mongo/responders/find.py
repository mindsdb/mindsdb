from bson.int64 import Int64
from collections import OrderedDict

from lightwood.api import dtype
from mindsdb_sql.parser.ast import *
import mindsdb.api.mongo.functions as helpers
from mindsdb.api.mongo.classes import Responder
from mindsdb.integrations.handlers.mongodb_handler.utils.mongodb_ast import MongoToAst
from mindsdb.api.mysql.mysql_proxy.classes.sql_query import SQLQuery
from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController

class Responce(Responder):
    when = {'find': helpers.is_true}

    def result(self, query, request_env, mindsdb_env, session):
        # system queries
        if query['find'] == 'system.version':
            # For studio3t
            data = [{
                "_id" : "featureCompatibilityVersion",
                "version" : "3.6"
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

        if not query.get('singleBatch') and 'collection' in query.get('filter'):
            # convert to join

            # upper query
            ast_query = mongoToAst.find(
                collection=query['find'],
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

            # convert to join
            predictor = ast_query.from_table

            ast_join = Join(
                left=table_select,
                right=predictor,
                join_type='join'
            )
            ast_query.from_table = ast_join

        else:
            # is single table
            ast_query = mongoToAst.find(
                collection=query['find'],
                filter=query.get('filter'),
                projection=query.get('projection'),
                sort=query.get('sort'),
                limit=query.get('limit'),
                skip=query.get('skip'),
            )

        session.original_integration_controller = mindsdb_env['origin_integration_controller']
        session.original_model_interface = mindsdb_env['origin_model_interface']
        session.original_view_controller = mindsdb_env['origin_view_controller']

        sql_session = SessionController(
            server=session,
            company_id=mindsdb_env['company_id']
        )

        sql_session.database = 'mindsdb'

        sql_query = SQLQuery(
            ast_query,
            session=sql_session
        )
        #
        result = sql_query.fetch(
            sql_session.datahub
        )

        column_names = [
            c.name is c.alias is None or c.alias
            for c in sql_query.columns_list
        ]

        data = []
        for row in result['result']:
            data.append(dict(zip(column_names, row)))

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
