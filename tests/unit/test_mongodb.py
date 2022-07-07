import threading
import unittest
from unittest.mock import patch

from pymongo import MongoClient
from mindsdb_sql import parse_sql

from mindsdb.integrations.handlers.mongodb_handler.utils.mongodb_render import MongodbRender
from mindsdb.integrations.handlers.mongodb_handler.utils.mongodb_parser import MongodbParser

# How to run:
#  env PYTHONPATH=./ pytest tests/unit/test_mongodb.py


class TestMongoDBConverters(unittest.TestCase):

    def test_ast_to_mongo(self):

        sql = '''
            select *
             from tbl1
            where x!=1 and c=2 and d>4  or e is not null
            order by d, e desc
        '''

        # sql to ast
        query = parse_sql(sql, 'mindsdb')
        mql = MongodbRender().to_mongo_query(query)

        expected_mql = '''
          db.tbl1.aggregate([
            {"$match": {"$or": [
                {"$and": [{"$and": [
                     {"x": {"$ne": 1}},
                     {"c": 2}]}, 
                     {"d": {"$gt": 4}}]}, 
                {"e": {"$ne": null}}]}}, 
            {"$sort": {"d": -1, "e": -1}}
          ])
        '''.replace('\n', '')

        # test ast to mongo
        assert mql.to_string().replace(' ', '') == expected_mql.replace(' ', '')

        # test parsing: mql to string and then string to mql
        mql_str = mql.to_string()

        assert MongodbParser().from_string(mql_str).to_string() == mql_str


        sql = '''
           select distinct a.b, a.c  from tbl1
           where x=1 
           limit 2
           offset 3
        '''

        query = parse_sql(sql, 'mindsdb')
        mql = MongodbRender().to_mongo_query(query)

        expected_mql = '''
            db.tbl1.aggregate([
               {"$match": {"x": 1}}, 
               {"$group": {
                   "_id": {"b": "$b", "c": "$c"}, 
                   "b": {"$first": "$b"}, 
                   "c": {"$first": "$c"}
               }},
               {"$project": {"_id": 0, "b": "$b", "c": "$c"}}, 
               {"$skip": 3}, 
               {"$limit": 2}
            ])
        '''.replace('\n', '')

        # test ast to mongo
        assert mql.to_string().replace(' ', '') == expected_mql.replace(' ', '')

        # TODO test group

        # TODO use in queries:  multiline, objectid, isodate


class TestMongoDBServer(unittest.TestCase):

    def test_mongo_server(self):

        # mock sqlquery
        with patch('mindsdb.api.mysql.mysql_proxy.classes.sql_query.SQLQuery') as mock_sqlquery:

            # start mongo server
            config = {
                "api": {
                    "mongodb": {"host": "127.0.0.1", "port": "47399", "database": "mindsdb"}
                },
            }

            from mindsdb.api.mongo.server import MongoServer

            server = MongoServer(config)
            server_thread = threading.Thread(
                name='Mongo server',
                target=server.serve_forever,
            )

            # mongo client
            client_con = MongoClient(port=int(config['api']['mongodb']['port']))

            try:
                server_thread.start()

                # TODO test create integration

                # TODO ======== test create predictor =========
                # client_con.mindsdb.predictors.insert(
                #     {
                #         "name": "house_sales_model_h1w4",
                #         "predict": "MA",
                #         "connection": "example_mongo",
                #         "select_data_query": {
                #             "collection": "house_sales",
                #             "call": [{
                #                 "method": "find",
                #                 "args": []
                #             }]
                #         },
                #         "training_options": {
                #             "timeseries_settings": {
                #                 "order_by": ["saledate"],
                #                 "group_by": ["bedrooms", "type"],
                #                 "horizon": 1,
                #                 "window": 4
                #             }
                #         }
                #     }
                # )

                # ==== test single row ===

                res = client_con.mindsdb.fish_model1.find(
                    {'length1': 10, 'type': 'a'}
                )
                res = list(res)  # to fetch

                ast = mock_sqlquery.call_args.args[0]

                expected_sql = '''
                  SELECT * FROM fish_model1
                  where length1=10 and type='a'
                '''
                assert parse_sql(expected_sql, 'mindsdb').to_string() == ast.to_string()

                # ==== test join ===
                mock_sqlquery.reset_mock()

                res = client_con.mindsdb.fish_model1.find({
                    'collection': 'mongo.fish',
                    'query':  {"Species": 'Pike'},
                 }
                )
                res = list(res)

                ast = mock_sqlquery.call_args.args[0]

                expected_sql = '''
                  SELECT * FROM 
                     (SELECT * FROM mongo.fish WHERE Species = 'Pike')
                     JOIN fish_model1
                '''
                assert parse_sql(expected_sql, 'mindsdb').to_string() == ast.to_string()

                # ==== test join TS ===

                mock_sqlquery.reset_mock()

                res = client_con.mindsdb.house_sales_model_h1w4.find(
                    {
                        "collection": "example_mongo.house_sales2",
                        "query": {
                            "$where": "this.saledate > latest and this.type = 'house' and this.bedrooms=2"
                        },
                    },
                    {
                        "house_sales_model_h1w4.saledate": "date",
                        "house_sales_model_h1w4.ma": "forecast"
                    }
                )
                res = list(res)

                ast = mock_sqlquery.call_args.args[0]

                expected_sql = '''
                  SELECT house_sales_model_h1w4.saledate as date, house_sales_model_h1w4.ma as forecast  FROM 
                     (
                       SELECT * FROM example_mongo.house_sales2
                        WHERE saledate > latest
                       and type = 'house' and bedrooms=2
                     )
                     JOIN house_sales_model_h1w4
                '''
                assert parse_sql(expected_sql, 'mindsdb').to_string() == ast.to_string()
            except Exception as e:
                raise e
            finally:
                client_con.close()

                # shutdown server
                server.shutdown()
                server_thread.join()
                server.server_close()


class TestMongoDBHandler(unittest.TestCase):

    def test_mongo_handler(self):

        # TODO how to test mongo handler
        #   test mysql query
        #   test mongo query
        pass




