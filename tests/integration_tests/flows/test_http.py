import requests
import unittest
import inspect
from random import randint
from pathlib import Path
from uuid import uuid1
import json

import pandas as pd

from common import (
    CONFIG_PATH,
    DATASETS_PATH,
    EXTERNAL_DB_CREDENTIALS,
    make_test_csv,
    run_environment
)
from mindsdb.api.mysql.mysql_proxy.mysql_proxy import RESPONSE_TYPE

rand = randint(0, pow(10, 12))
ds_name = f'hr_ds_{rand}'
pred_name = f'hr_predictor_{rand}'
root = 'http://127.0.0.1:47334/api'


class HTTPTest(unittest.TestCase):
    @staticmethod
    def get_files_list():
        response = requests.request('GET', f'{root}/files/')
        assert response.status_code == 200
        response_data = response.json()
        assert isinstance(response_data, list)
        return response_data

    @classmethod
    def setUpClass(cls):
        run_environment(
            apis=['http'],
            override_config={
                'integrations': {
                    'default_mariadb': {
                        'publish': False
                    },
                    'default_clickhouse': {
                        'publish': False
                    }
                }
            }
        )

        cls.config = json.loads(
            Path(CONFIG_PATH).read_text()
        )

        cls.initial_integrations_names = list(cls.config['integrations'].keys())

        cls._sql_via_http_context = {}

        with open(EXTERNAL_DB_CREDENTIALS, 'rt') as f:
            db_creds = json.load(f)
            cls.sql_db_creds = {
                k: v for k, v in db_creds.items() if k in [
                    'mariadb', 'mysql', 'postgres',
                    'snowflake', 'scylla', 'cassandra'
                    # TODO mssql clickhouse
                ]
            }

    def sql_via_http(self, request: str, expected_resp_type: str = None, context: dict = None) -> dict:
        if context is None:
            context = self._sql_via_http_context
        response = requests.post(
            f'{root}/sql/query',
            json={
                'query': request,
                'context': context
            }
        )
        self.assertTrue(response.status_code == 200)
        response = response.json()
        self.assertTrue(
            response.get('type') == (expected_resp_type or [RESPONSE_TYPE.OK, RESPONSE_TYPE.TABLE, RESPONSE_TYPE.ERROR])
        )
        self.assertIsInstance(response.get('context'), dict)
        if response['type'] == 'table':
            self.assertIsInstance(response.get('data'), list)
            self.assertIsInstance(response.get('column_names'), list)
        elif response['type'] == 'error':
            self.assertIsInstance(response.get('error_code'), int)
            self.assertIsInstance(response.get('error_message'), str)
        self._sql_via_http_context = response['context']
        return response

    def show_databases(self):
        resp = self.sql_via_http('show databases', RESPONSE_TYPE.TABLE)
        return [x[0] for x in resp['data']]

    # def test_1_config(self):
    #     ''' obsolete
    #     '''
    #     print(f'\nExecuting {inspect.stack()[0].function}')
    #     res = requests.get(f'{root}/config/integrations')
    #     assert res.status_code == 200
    #     res = res.json()
    #     assert isinstance(res['integrations'], list)

    #     test_integration_data = {'publish': False, 'host': 'test', 'type': 'clickhouse', 'port': 9000, 'user': 'default', 'password': '123'}
    #     res = requests.put(f'{root}/config/integrations/test_integration', json={'params': test_integration_data})
    #     assert res.status_code == 200

    #     res = requests.get(f'{root}/config/integrations/test_integration')
    #     assert res.status_code == 200
    #     test_integration = res.json()
    #     print(test_integration)
    #     assert len(test_integration) == 10

    #     for k in test_integration_data:
    #         if k != 'password':
    #             assert test_integration[k] == test_integration_data[k]

    #     for name in ['test_integration']:
    #         # Get the original
    #         res = requests.get(f'{root}/config/integrations/{name}')
    #         assert res.status_code == 200

    #         integration = res.json()
    #         for k in ['publish', 'host', 'port', 'type', 'user']:
    #             assert k in integration
    #             assert integration[k] is not None
    #         assert integration.get('password') is None

    #         # Modify it
    #         res = requests.post(
    #             f'{root}/config/integrations/{name}',
    #             json={'params': {'user': 'dr.Who'}}
    #         )

    #         res = requests.get(f'{root}/config/integrations/{name}')
    #         assert res.status_code == 200
    #         modified_integration = res.json()

    #         assert modified_integration['password'] is None
    #         assert modified_integration['user'] == 'dr.Who'
    #         for k in integration:
    #             if k not in ['password', 'date_last_update', 'user']:
    #                 assert modified_integration[k] == integration[k]

    #         # Put the original values back in\
    #         del integration['password']
    #         res = requests.post(f'{root}/config/integrations/{name}', json={'params': integration})
    #         res = requests.get(f'{root}/config/integrations/{name}')
    #         assert res.status_code == 200
    #         modified_integration = res.json()
    #         for k in integration:
    #             if k != 'date_last_update':
    #                 assert modified_integration[k] == integration[k]

    #     res = requests.delete(f'{root}/config/integrations/test_integration')
    #     assert res.status_code == 200

    #     res = requests.get(f'{root}/config/integrations/test_integration')
    #     assert res.status_code != 200

    def test_00_files(self):
        ''' sql-via-http:
            upload file
            delete file
            upload file again
        '''
        print(f'\nExecuting {inspect.stack()[0].function}')
        files_list = self.get_files_list()
        self.assertTrue(len(files_list) == 0)

        if Path('train.csv').is_file() is False:
            resp = requests.get('https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/classics/home_rentals/dataset/train.csv')
            with open('tests/train.csv', 'wb') as f:
                f.write(resp.content)

        file_path = Path('tests/train.csv')
        df = pd.read_csv(file_path)
        test_csv_path = make_test_csv('test_home_rentals.csv', df.head(50))

        with open(test_csv_path) as td:
            files = {
                'file': ('test_data.csv', td, 'text/csv'),
                'original_file_name': (None, 'super_test_data.csv')  # optional
            }

            response = requests.request('PUT', f'{root}/files/test_file', files=files, json=None, params=None, data=None)
            self.assertTrue(response.status_code == 200)

        files_list = self.get_files_list()
        self.assertTrue(files_list[0]['name'] == 'test_file')

        response = requests.delete(f'{root}/files/test_file')
        self.assertTrue(response.status_code == 200)

        files_list = self.get_files_list()
        self.assertTrue(len(files_list) == 0)

        with open(test_csv_path) as td:
            files = {
                'file': ('test_data.csv', td, 'text/csv'),
                'original_file_name': (None, 'super_test_data.csv')  # optional
            }

            response = requests.request('PUT', f'{root}/files/test_file', files=files, json=None, params=None, data=None)
            self.assertTrue(response.status_code == 200)

        files_list = self.get_files_list()
        self.assertTrue(files_list[0]['name'] == 'test_file')

    def test_01_sql_general_syntax(self):
        ''' test sql in general
        '''
        select_const_int = [
            'select 1',
            'select 1;',
            'SELECT 1',
            'Select 1',
            '   select   1   ',
            '''   select
                  1;
            '''
        ]
        select_const_int_alias = [
            'select 1 as `2`',
            # "select 1 as '2'",     https://github.com/mindsdb/mindsdb_sql/issues/198
            'select 1 as "2"',
            'select 1 `2`',
            # "select 1 '2'",     https://github.com/mindsdb/mindsdb_sql/issues/198
            'select 1 "2"',
        ]
        select_const_str = [
            'select "a"',
            "select 'a'"
        ]
        select_const_str_alias = [
            'select "a" as b',
            "select 'a' as b",
            'select "a" b',
            # 'select "a" "b"',   # => ab
            'select "a" `b`',
            # "select 'a' 'b'"    # => ab
        ]
        bunch = [{
            'queries': select_const_int,
            'result': 1,
            'alias': '1'
        }, {
            'queries': select_const_int_alias,
            'result': 1,
            'alias': '2'
        }, {
            'queries': select_const_str,
            'result': 'a',
            'alias': 'a'
        }, {
            'queries': select_const_str_alias,
            'result': 'a',
            'alias': 'b'
        }]
        for group in bunch:
            queries = group['queries']
            expected_result = group['result']
            expected_alias = group['alias']
            for query in queries:
                print(query)
                resp = self.sql_via_http(query, RESPONSE_TYPE.TABLE)
                try:
                    self.assertTrue(len(resp['column_names']) == 1)
                    self.assertTrue(resp['column_names'][0] == expected_alias)
                    self.assertTrue(len(resp['data']) == 1)
                    self.assertTrue(len(resp['data'][0]) == 1)
                    self.assertTrue(resp['data'][0][0] == expected_result)
                except Exception:
                    print(f'Error in query: {query}')
                    raise

    def test_02_context_changing(self):
        resp = self.sql_via_http('use mindsdb', RESPONSE_TYPE.OK)
        self.assertTrue(resp['context']['db'] == 'mindsdb')

        resp_1 = self.sql_via_http('show tables', RESPONSE_TYPE.TABLE)
        table_names = [x[0] for x in resp_1['data']]
        self.assertTrue('test_file' not in table_names)
        self.assertTrue('predictors' in table_names)

        resp = self.sql_via_http('use files', RESPONSE_TYPE.OK)
        self.assertTrue(resp['context']['db'] == 'files')

        resp_4 = self.sql_via_http('show tables', RESPONSE_TYPE.TABLE)
        table_names = [x[0] for x in resp_4['data']]
        self.assertTrue('test_file' in table_names)
        self.assertTrue('predictors' not in table_names)

    def test_03_special_queries(self):
        # "show databases;",
        # "show schemas;",
        # "show tables;",
        # "show tables from mindsdb;",
        # "show full tables from mindsdb;",
        # "show variables;",
        # "show session status;",
        # "show global variables;",
        # "show engines;",
        # "show warnings;",
        # "show charset;",
        # "show collation;",
        # "show datasources;",      # !!!
        # "show predictors;",       # !!!
        # "show function status where db = 'mindsdb';",
        # "show procedure status where db = 'mindsdb';",
        empty_table = [
            "show function status",
            "show function status where db = 'mindsdb'",
            "show procedure status",
            "show procedure status where db = 'mindsdb'",
            "show warnings"
        ]
        for query in empty_table:
            try:
                print(query)
                resp = self.sql_via_http(query, RESPONSE_TYPE.TABLE)
                self.assertTrue(len(resp['data']) == 0)
            except Exception:
                print(f'Error in query: {query}')
                raise

        not_empty_table = [
            "show databases",
            "show schemas",
            "show variables",
            "show session status",
            "show global variables",
            "show engines",
            "show charset",
            "show collation"
        ]
        for query in not_empty_table:
            try:
                print(query)
                resp = self.sql_via_http(query, RESPONSE_TYPE.TABLE)
                self.assertTrue(len(resp['data']) > 0)
            except Exception:
                print(f'Error in query: {query}')
                raise

        # show databaes should be same as show schemas
        try:
            query = 'show databases'
            resp = self.sql_via_http(query, RESPONSE_TYPE.TABLE)
            self.assertTrue(len(resp['column_names']) == 1)
            self.assertTrue(resp['column_names'][0] == 'Database')
            db_names = [x[0].lower() for x in resp['data']]
            self.assertTrue('information_schema' in db_names)
            self.assertTrue('mindsdb' in db_names)
            self.assertTrue('files' in db_names)
        except Exception:
            print(f'Error in query: {query}')
            raise

    def test_04_show_tables(self):
        self.sql_via_http('use mindsdb', RESPONSE_TYPE.OK)

        resp_1 = self.sql_via_http('show tables', RESPONSE_TYPE.TABLE)
        resp_2 = self.sql_via_http('show tables from mindsdb', RESPONSE_TYPE.TABLE)
        resp_3 = self.sql_via_http('show full tables from mindsdb', RESPONSE_TYPE.TABLE)
        self.assertTrue(
            resp_1['data'].sort() == resp_2['data'].sort()
            and resp_1['data'].sort() == resp_3['data'].sort()
        )

    def test_05_sql_create_database(self):
        ''' sql-via-http:
            'create datasource' for each db (obsolete?)
            'drop datasource' for each db (obsolete?)
            'create database' for each db
            'drop database' for each db
            'create database' for each db
        '''
        print(f'\nExecuting {inspect.stack()[0].function}')
        created_db_names = []
        for db_type, db_creds in self.sql_db_creds.items():
            queries = [
                {
                    'create': 'CREATE DATASOURCE',
                    'drop': 'DROP DATASOURCE'
                }, {
                    'create': 'CREATE DATABASE',
                    'drop': 'DROP DATABASE'
                }, {
                    'create': 'CREATE DATABASE',
                    'drop': None
                }
            ]
            for query in queries:
                create_query = query['create']
                drop_query = query['drop']
                db_name = db_type.upper()
                created_db_names.append(db_name)
                with self.subTest(msg=f'{db_type}'):
                    print(f"\nExecuting {self._testMethodName} ({__name__}.{self.__class__.__name__}) [{db_name}]")
                    query = f"""
                        {create_query} {db_name}
                        WITH ENGINE = '{db_type}',
                        PARAMETERS = {json.dumps(db_creds)};
                    """
                    self.sql_via_http(query, RESPONSE_TYPE.OK)
                    self.assertTrue(db_name in self.show_databases())
                    if drop_query is not None:
                        self.sql_via_http(f'{drop_query} {db_name}', RESPONSE_TYPE.OK)
                        self.assertTrue(db_name.upper() not in self.show_databases())

        for query in ['show databases', 'show datasources']:
            resp = self.sql_via_http(query, RESPONSE_TYPE.TABLE)
            db_names = [x[0] for x in resp['data']]
            for name in created_db_names:
                self.assertTrue(name in db_names)

    def test_06_sql_select_from_file(self):
        self.sql_via_http('use mindsdb', RESPONSE_TYPE.OK)
        resp = self.sql_via_http('select * from files.test_file', RESPONSE_TYPE.TABLE)
        self.assertTrue(len(resp['data']) == 50)
        self.assertTrue(len(resp['column_names']) == 8)

        resp = self.sql_via_http('select rental_price, rental_price as rp1, rental_price rp2 from files.test_file limit 10', RESPONSE_TYPE.TABLE)
        self.assertTrue(len(resp['data']) == 10)
        self.assertTrue(resp['column_names'] == ['rental_price', 'rp1', 'rp2'])
        self.assertTrue(resp['data'][0][0] == resp['data'][0][1] and resp['data'][0][0] == resp['data'][0][2])

    def test_07_sql_create_predictor(self):
        resp = self.sql_via_http('show predictors', RESPONSE_TYPE.TABLE)
        self.assertTrue(len(resp['data']) == 0)

        # self.sql_via_http('create predictor test1 from files (select * from test_file) ', RESPONSE_TYPE.TABLE)
        # TODO

    def test_08_utils(self):
        """
        Call utilities ping endpoint
        THEN check the response is success
        """

        response = requests.get(f'{root}/util/ping')
        assert response.status_code == 200

        response = requests.get(f'{root}/util/ping_native')
        assert response.status_code == 200

        response = requests.get(f'{root}/config/vars')
        assert response.status_code == 200

    def test_09_predictors(self):
        """
        Call list predictors endpoint
        THEN check the response is success
        """
        response = requests.get(f'{root}/predictors/')
        assert response.status_code == 200

    def test_010_predictor_not_found(self):
        """
        Call unexisting predictor
        then check the response is NOT FOUND
        """
        response = requests.get(f'{root}/predictors/dummy_predictor')
        assert response.status_code != 200

    def test_11_gui_is_served(self):
        """
        GUI downloaded and available
        """
        response = requests.get('http://localhost:47334/')
        assert response.status_code == 200
        assert response.content.decode().find('<head>') > 0

    # def test_93_generate_predictor(self):
    #     r = requests.put(
    #         f'{root}/predictors/generate/lwr_{pred_name}',
    #         json={
    #             'problem_definition': {'target': 'rental_price'},
    #             'data_source_name': ds_name,
    #             'join_learn_process': True
    #         }
    #     )
    #     r.raise_for_status()

    # def test_94_edit_json_ai(self):
    #     # Get the json ai
    #     resp = requests.get(f'{root}/predictors/lwr_{pred_name}')
    #     predictor_data = resp.json()

    #     # Edit it
    #     json_ai = predictor_data['json_ai']
    #     json_ai['problem_definition']
    #     mixers = json_ai['model']['args']['submodels']
    #     keep_only = [x for x in mixers if x['module'] != 'Regression']
    #     json_ai['model']['args']['submodels'] = keep_only

    #     # Upload it
    #     r = requests.put(
    #         f'{root}/predictors/lwr_{pred_name}/edit/json_ai',
    #         json={'json_ai': json_ai}
    #     )
    #     r.raise_for_status()

    # def test_95_validate_json_ai(self):
    #     # Get the json ai
    #     resp = requests.get(f'{root}/predictors/lwr_{pred_name}')
    #     predictor_data = resp.json()

    #     # Check it
    #     r = requests.post(
    #         f'{root}/util/validate_json_ai',
    #         json={'json_ai': predictor_data['json_ai']}
    #     )
    #     r.raise_for_status()

    # def test_96_edit_code(self):
    #     # Make sure json ai edits went through
    #     resp = requests.get(f'{root}/predictors/lwr_{pred_name}')
    #     predictor_data = resp.json()
    #     assert 'Regression(' not in predictor_data['code']

    #     # Change the code
    #     new_code = predictor_data['code']
    #     new_code = new_code.split('''self.mode = "predict"''')[0]
    #     new_code += """\n        return pd.DataFrame({'prediction': [int(5555555)]}).astype(int)"""

    #     r = requests.put(
    #         f'{root}/predictors/lwr_{pred_name}/edit/code',
    #         json={'code': new_code}
    #     )
    #     r.raise_for_status()

    # def test_97_train_predictor(self):
    #     r = requests.put(
    #         f'{root}/predictors/lwr_{pred_name}/train',
    #         json={'data_source_name': ds_name, 'join_learn_process': True}
    #     )
    #     r.raise_for_status()

    # def test_98_predict_modified_predictor(self):
    #     params = {
    #         'when': {'sqft': 500}
    #     }
    #     url = f'{root}/predictors/lwr_{pred_name}/predict'
    #     res = requests.post(url, json=params)
    #     assert res.status_code == 200
    #     pvs = res.json()
    #     assert pvs[0]['rental_price']['predicted_value'] == 5555555

    # def test_99_export_and_import_predictor(self):
    #     # Create and train a new predictor
    #     params = {
    #         'data_source_name': ds_name,
    #         'to_predict': 'rental_price',
    #         'kwargs': {
    #             'stop_training_in_x_seconds': 20,
    #             'join_learn_process': True
    #         }
    #     }
    #     url = f'{root}/predictors/test_99_{pred_name}'
    #     res = requests.put(url, json=params)
    #     assert res.status_code == 200
    #
    #     # Export the predictor as a binary
    #     res = requests.get(f'{root}/predictors/test_99_{pred_name}/export')
    #     assert res.status_code == 200
    #     exported_predictor = json.loads(res.text)  # undo the extra wrapping done by requests
    #
    #     # Delete the predictor
    #     res = requests.delete(f'{root}/predictors/test_99_{pred_name}')
    #     assert res.status_code == 200
    #
    #     # Import the predictor from the previous export
    #     res = requests.put(f'{root}/predictors/test_99_{pred_name}/import', json=exported_predictor)
    #     assert res.status_code == 200
    #
    #     # Test that it still exists and that it can make predictions
    #     url = f'{root}/predictors/test_99_{pred_name}/predict'
    #     res = requests.post(url, json={'when': {'sqft': 500}})
    #     assert res.status_code == 200
    #     assert isinstance(res.json()[0]['rental_price']['predicted_value'], float)

if __name__ == '__main__':
     unittest.main(failfast=True)
