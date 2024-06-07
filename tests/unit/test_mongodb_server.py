import datetime as dt
import json
import threading
import inspect
from unittest.mock import patch
import tempfile
import os

from pymongo import MongoClient
from mindsdb_sql import parse_sql

from mindsdb.api.executor.data_types.answer import ExecuteAnswer
from mindsdb.api.executor import Column, ResultSet

# How to run:
#  env PYTHONPATH=./ pytest tests/unit/test_mongodb_server.py

from .executor_test_base import BaseUnitTest


class TestMongoDBServer(BaseUnitTest):

    def test_mongo_server(self):

        # mock sqlquery
        with patch('mindsdb.api.executor.command_executor.ExecuteCommands.execute_command') as mock_executor:

            # if this module was imported in other test it prevents mocking SQLQuery inside MongoServer thread
            # unload_module('mindsdb.api.mysql.mysql_proxy.executor.executor_commands')

            # start mongo server
            config = {
                "api": {
                    "mongodb": {"host": "127.0.0.1", "port": "47399", "database": "mindsdb"}
                },
            }
            # TODO run on own database
            fdi, cfg_file = tempfile.mkstemp(prefix='mindsdb_conf_')

            with os.fdopen(fdi, 'w') as fd:
                json.dump(config, fd)

            os.environ['MINDSDB_CONFIG_PATH'] = cfg_file

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

                # run all methods with prefix t_
                for test_name, test_method in inspect.getmembers(self, predicate=inspect.ismethod):
                    if test_name.startswith('t_'):
                        mock_executor.reset_mock()
                        mock_executor.side_effect = lambda x: ExecuteAnswer()

                        test_method(client_con, mock_executor)

            except Exception as e:
                raise e
            finally:
                client_con.close()

                # shutdown server
                server.shutdown()
                server_thread.join()
                server.server_close()

    def t_single_row(self, client_con, mock_executor):
        # ==== test single row ===
        mock_executor.side_effect = lambda x: ExecuteAnswer(data=ResultSet(columns=[Column('a')], values=[['test']]))

        res = client_con.mindsdb.fish_model1.find(
            {'length1': 10, 'type': 'a'}
        )
        res = list(res)  # to fetch
        assert res == [{'a': 'test'}]

        ast = mock_executor.call_args[0][0]

        expected_sql = '''
          SELECT * FROM mindsdb.fish_model1
          where length1=10 and type='a'
        '''
        assert parse_sql(expected_sql, 'mindsdb').to_string() == ast.to_string()

    def t_single_join(self, client_con, mock_executor):
        # ==== test join ===

        res = client_con.mindsdb.fish_model1.find({
            'collection': 'mongo.fish',
            'query': {"Species": 'Pike'},
        }
        )
        res = list(res)

        ast = mock_executor.call_args[0][0]

        expected_sql = '''
          SELECT * FROM 
             (SELECT * FROM mongo.fish WHERE Species = 'Pike') as fish
             JOIN mindsdb.fish_model1
        '''
        assert parse_sql(expected_sql, 'mindsdb').to_string() == ast.to_string()

    def t_join_ts(self, client_con, mock_executor):
        # ==== test join TS ===

        modifiers = [{'$unwind': '$hist_data'}]
        res = client_con.mindsdb.house_sales_model_h1w4.find(
            {
                "collection": "example_mongo.house_sales2",
                "query": {
                    "$where": "this.saledate > latest and this.type = 'house' and this.bedrooms=2"
                },
                "modifiers": modifiers,
            },
            {
                "house_sales_model_h1w4.saledate": "date",
                "house_sales_model_h1w4.ma": "forecast"
            }
        )
        res = list(res)

        ast = mock_executor.call_args[0][0]

        expected_sql = '''
          SELECT house_sales_model_h1w4.saledate as date, house_sales_model_h1w4.ma as forecast  FROM 
             (
               SELECT * FROM example_mongo.house_sales2
                WHERE saledate > latest
               and type = 'house' and bedrooms=2
             ) as house_sales2
             JOIN mindsdb.house_sales_model_h1w4
        '''
        assert parse_sql(expected_sql, 'mindsdb').to_string() == ast.to_string()

        # check modifiers
        assert ast.from_table.left.modifiers == modifiers

    def t_datetime(self, client_con, mock_executor):
        # ==== test datetime ===

        res = client_con.mongo.house_sales.find({'saledate': {'$gt': dt.datetime.fromisoformat("2018-03-31T00:00:00")}})
        res = list(res)

        ast = mock_executor.call_args[0][0]

        expected_sql = "SELECT * FROM mongo.house_sales WHERE saledate > '2018-03-31 00:00:00'"
        assert parse_sql(expected_sql, 'mindsdb').to_string() == ast.to_string()

    def t_create_predictor(self, client_con, mock_executor):
        res = client_con.myproj.predictors.insert_one(
            {
                "name": "house_sales_model5",
                "predict": "ma",
                "connection": "mongo",
                "select_data_query": "db.house_sales.find({})",
                "training_options": {
                    "timeseries_settings": {
                        "order_by": "saledate",
                        "group_by": ["bedrooms", "type"],
                        "horizon": 4,
                        "window": 4
                    },
                    "encoders.location.module": "CategoricalAutoEncoder",
                }
            }
        )
        ast = mock_executor.call_args[0][0]

        expected_sql = '''
           CREATE PREDICTOR myproj.house_sales_model5 
           FROM mongo (
                db.house_sales.find({})
           ) 
           PREDICT ma 
           ORDER BY saledate 
           GROUP BY bedrooms, type 
           WINDOW 4 
           HORIZON 4 
           USING encoders.location.module="CategoricalAutoEncoder"
        '''
        assert parse_sql(expected_sql, 'mindsdb').to_string() == ast.to_string()

    def t_delete_model(self, client_con, mock_executor):

        expected_sql = "DROP PREDICTOR myproj.house_sales_model5"

        client_con.myproj.models.delete_one({'name': 'house_sales_model5'})
        ast = mock_executor.call_args[0][0]

        assert parse_sql(expected_sql, 'mindsdb').to_string() == ast.to_string()

        # the same with 'predictors' table
        mock_executor.reset_mock()

        client_con.myproj.predictors.delete_one({'name': 'house_sales_model5'})
        ast = mock_executor.call_args[0][0]

        assert parse_sql(expected_sql, 'mindsdb').to_string() == ast.to_string()

    def t_delete_model_version(self, client_con, mock_executor):

        client_con.myproj.models.delete_one({'name': 'house_sales_model5', 'version': 112})

        ast = mock_executor.call_args[0][0]

        expected_sql = "DROP MODEL myproj.house_sales_model5.112"
        assert parse_sql(expected_sql, 'mindsdb').to_string() == ast.to_string()

    # ml engines

    def t_create_ml_engine(self, client_con, mock_executor):

        client_con.myproj.ml_engines.insert_one({'name': "openai2", "handler": "openai", "params": {"openai_api_key": "qqq"}})

        ast = mock_executor.call_args[0][0]

        expected_sql = "CREATE ML_ENGINE openai2 FROM openai USING openai_api_key= 'qqq'"
        assert parse_sql(expected_sql, 'mindsdb').to_string() == ast.to_string()

    def t_list_ml_engines(self, client_con, mock_executor):

        res = client_con.myproj.ml_engines.find({})
        res = list(res)

        ast = mock_executor.call_args[0][0]

        expected_sql = "SHOW ML_ENGINES"
        assert parse_sql(expected_sql, 'mindsdb').to_string() == ast.to_string()

    def t_drop_ml_engine(self, client_con, mock_executor):

        client_con.myproj.ml_engines.delete_one({'name': 'openai2'})

        ast = mock_executor.call_args[0][0]

        expected_sql = "DROP ML_ENGINE openai2"
        assert parse_sql(expected_sql, 'mindsdb').to_string() == ast.to_string()

    def t_delete_model_version_by_id(self, client_con, mock_executor):
        # TODO
        #   delete model and version by _id
        #   need to mock model_controller
        pass
