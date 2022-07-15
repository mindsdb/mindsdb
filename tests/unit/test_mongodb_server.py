import datetime as dt
import threading
import unittest
from unittest.mock import patch

from pymongo import MongoClient
from mindsdb_sql import parse_sql


# How to run:
#  env PYTHONPATH=./ pytest tests/unit/test_mongodb_server.py


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
                  SELECT * FROM mindsdb.fish_model1
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
                     JOIN mindsdb.fish_model1
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
                     JOIN mindsdb.house_sales_model_h1w4
                '''
                assert parse_sql(expected_sql, 'mindsdb').to_string() == ast.to_string()

                # ==== test datetime ===

                mock_sqlquery.reset_mock()

                res = client_con.mongo.house_sales.find({'saledate': {'$gt': dt.datetime.fromisoformat("2018-03-31T00:00:00")}})
                res = list(res)

                ast = mock_sqlquery.call_args.args[0]

                expected_sql = "SELECT * FROM mongo.house_sales WHERE saledate > '2018-03-31 00:00:00'"
                assert parse_sql(expected_sql, 'mindsdb').to_string() == ast.to_string()

            except Exception as e:
                raise e
            finally:
                client_con.close()

                # shutdown server
                server.shutdown()
                server_thread.join()
                server.server_close()





