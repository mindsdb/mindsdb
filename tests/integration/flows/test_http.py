import json
from typing import List

import requests
import pytest

from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from tests.utils.config import HTTP_API_ROOT
from tests.utils.http_test_helpers import HTTPHelperMixin


class TestHTTP(HTTPHelperMixin):
    @staticmethod
    def get_files_list():
        response = requests.request('GET', f'{HTTP_API_ROOT}/files/')
        assert response.status_code == 200
        response_data = response.json()
        assert isinstance(response_data, list)
        return response_data

    @classmethod
    def setup_class(cls):
        cls._sql_via_http_context = {}

    def create_database(self, name, db_data):
        db_type = db_data["type"]
        # Drop any existing DB with this name to avoid conflicts
        self.sql_via_http(f'DROP DATABASE IF EXISTS {name};', RESPONSE_TYPE.OK)
        self.sql_via_http(f"CREATE DATABASE {name} WITH ENGINE = '{db_type}', PARAMETERS = {json.dumps(db_data['connection_data'])};", RESPONSE_TYPE.OK)

    def validate_database_creation(self, name):
        res = self.sql_via_http(f"SELECT name FROM information_schema.databases WHERE name='{name}';")
        assert name in res["data"][0], f"Expected datasource is not found after creation - {name}: {res}"

    @pytest.mark.parametrize("util_uri", ["util/ping", "util/ping_native"])
    def test_utils(self, util_uri):
        """
        Call utilities ping endpoint
        THEN check the response is success
        """

        path = f"{HTTP_API_ROOT}/{util_uri}"
        response = requests.get(path)
        assert response.status_code == 200

    def test_auth(self):
        session = requests.Session()

        response = session.get(f'{HTTP_API_ROOT}/status')
        assert response.status_code == 200
        assert response.json()['auth']['http_auth_enabled'] is False

        response = session.get(f'{HTTP_API_ROOT}/config/')
        assert response.status_code == 200
        assert response.json()['auth']['http_auth_enabled'] is False

        response = session.get(f'{HTTP_API_ROOT}/tree/')
        assert response.status_code == 200

        response = session.put(f'{HTTP_API_ROOT}/config/', json={
            'http_auth_enabled': True,
            'username': '',
            'password': ''
        })
        assert response.status_code == 400

        response = session.put(f'{HTTP_API_ROOT}/config/', json={
            'auth': {
                'http_auth_enabled': True,
                'username': 'mindsdb',
                'password': 'mindsdb'
            }
        })
        assert response.status_code == 200

        response = session.get(f'{HTTP_API_ROOT}/status')
        assert response.status_code == 200
        assert response.json()['auth']['http_auth_enabled'] is True

        response = session.get(f'{HTTP_API_ROOT}/tree/')
        assert response.status_code == 401

        response = session.post(f'{HTTP_API_ROOT}/login', json={
            'username': 'mindsdb',
            'password': 'mindsdb'
        })
        assert response.status_code == 200

        response = session.get(f'{HTTP_API_ROOT}/tree/')
        assert response.status_code == 200

        response = session.post(f'{HTTP_API_ROOT}/logout')
        assert response.status_code == 200

        response = session.get(f'{HTTP_API_ROOT}/tree/')
        assert response.status_code == 401

        response = session.post(f'{HTTP_API_ROOT}/login', json={
            'username': 'mindsdb',
            'password': 'mindsdb'
        })
        assert response.status_code == 200

        response = session.put(f'{HTTP_API_ROOT}/config/', json={
            'auth': {
                'http_auth_enabled': False,
                'username': 'mindsdb',
                'password': ''
            }
        })

        response = session.get(f'{HTTP_API_ROOT}/status')
        assert response.status_code == 200
        assert response.json()['auth']['http_auth_enabled'] is False

    def test_gui_is_served(self):
        """
        GUI downloaded and available
        """
        response = requests.get(HTTP_API_ROOT.split("/api")[0])
        assert response.status_code == 200
        assert response.content.decode().find('<head>') > 0

    def test_files(self):
        ''' sql-via-http:
            upload file
            delete file
            upload file again
        '''
        self.sql_via_http("DROP TABLE IF EXISTS files.movies;", RESPONSE_TYPE.OK)
        assert "movies" not in [file["name"] for file in self.get_files_list()]

        with open("tests/data/movies.csv") as f:
            files = {
                'file': ('movies.csv', f, 'text/csv')
            }

            response = requests.request('PUT', f'{HTTP_API_ROOT}/files/movies', files=files)
            assert response.status_code == 200

        assert "movies" in [file["name"] for file in self.get_files_list()]

        response = requests.delete(f'{HTTP_API_ROOT}/files/movies')
        assert response.status_code == 200
        assert "movies" not in [file["name"] for file in self.get_files_list()]

        # Upload the file again (to guard against bugs where we still think a deleted file exists)
        with open("tests/data/movies.csv") as f:
            files = {
                'file': ('movies.csv', f, 'text/csv')
            }

            response = requests.request('PUT', f'{HTTP_API_ROOT}/files/movies', files=files)
            assert response.status_code == 200

    def test_sql_select_from_file(self):
        self.sql_via_http('use mindsdb', RESPONSE_TYPE.OK)
        resp = self.sql_via_http('select * from files.movies', RESPONSE_TYPE.TABLE)
        assert len(resp['data']) == 10
        assert len(resp['column_names']) == 3

        resp = self.sql_via_http('select title, title as t1, title t2 from files.movies limit 10', RESPONSE_TYPE.TABLE)
        assert len(resp['data']) == 10
        assert resp['column_names'] == ['title', 't1', 't2']
        assert resp['data'][0][0] == resp['data'][0][1] and resp['data'][0][0] == resp['data'][0][2]

    def test_sql_general_syntax(self):
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
                    assert len(resp['column_names']) == 1
                    assert resp['column_names'][0] == expected_alias
                    assert len(resp['data']) == 1
                    assert len(resp['data'][0]) == 1
                    assert resp['data'][0][0] == expected_result
                except Exception:
                    print(f'Error in query: {query}')
                    raise

    def test_context_changing(self):
        resp = self.sql_via_http('use mindsdb', RESPONSE_TYPE.OK)
        assert resp['context']['db'] == 'mindsdb'

        resp_1 = self.sql_via_http('show tables', RESPONSE_TYPE.TABLE)
        table_names = [x[0] for x in resp_1['data']]
        assert 'movies' not in table_names
        assert 'models' in table_names

        resp = self.sql_via_http('use files', RESPONSE_TYPE.OK)
        assert resp['context']['db'] == 'files'

        resp_4 = self.sql_via_http('show tables', RESPONSE_TYPE.TABLE)
        table_names = [x[0] for x in resp_4['data']]
        assert 'movies' in table_names
        assert 'models' not in table_names

    @pytest.mark.parametrize("query", [
        "show function status",
        "show function status where db = 'mindsdb'",
        "show procedure status",
        "show procedure status where db = 'mindsdb'",
        "show warnings"
    ])
    def test_special_queries_empty_table(self, query):
        resp = self.sql_via_http(query, RESPONSE_TYPE.TABLE)
        assert len(resp['data']) == 0

    @pytest.mark.parametrize("query", [
        "show databases",
        "show schemas",
        "show variables",
        "show session status",
        "show global variables",
        "show engines",
        "show charset",
        "show collation"
    ])
    def test_special_queries_not_empty_table(self, query):
        resp = self.sql_via_http(query, RESPONSE_TYPE.TABLE)
        assert len(resp['data']) > 0

    def test_special_queries_show_databases(self):
        query = 'show databases'
        resp = self.sql_via_http(query, RESPONSE_TYPE.TABLE)
        assert len(resp['column_names']) == 1
        assert resp['column_names'][0] == 'Database'
        db_names = [x[0].lower() for x in resp['data']]
        assert 'information_schema' in db_names
        assert 'mindsdb' in db_names
        assert 'files' in db_names

    def test_show_tables(self):
        self.sql_via_http('use mindsdb', RESPONSE_TYPE.OK)
        resp_1 = self.sql_via_http('show tables', RESPONSE_TYPE.TABLE)
        resp_2 = self.sql_via_http('show tables from mindsdb', RESPONSE_TYPE.TABLE)
        resp_3 = self.sql_via_http('show full tables from mindsdb', RESPONSE_TYPE.TABLE)
        assert resp_1['data'].sort() == resp_2['data'].sort()
        assert resp_1['data'].sort() == resp_3['data'].sort()

    def test_create_postgres_datasources(self):
        db_details = {
            "type": "postgres",
            "connection_data": {
                "type": "postgres",
                "host": "samples.mindsdb.com",
                "port": "5432",
                "user": "demo_user",
                "password": "demo_password",
                "database": "demo"
            }
        }
        self.create_database("test_http_postgres", db_details)
        self.validate_database_creation("test_http_postgres")

    def test_create_mariadb_datasources(self):
        db_details = {
            "type": "mariadb",
            "connection_data": {
                "type": "mariadb",
                "host": "samples.mindsdb.com",
                "port": "3307",
                "user": "demo_user",
                "password": "demo_password",
                "database": "test_data"
            }
        }
        self.create_database("test_http_mariadb", db_details)
        self.validate_database_creation("test_http_mariadb")

    def test_create_mysql_datasources(self):
        db_details = {
            "type": "mysql",
            "connection_data": {
                "type": "mysql",
                "host": "samples.mindsdb.com",
                "port": "3306",
                "user": "user",
                "password": "MindsDBUser123!",
                "database": "public"
            }
        }
        self.create_database("test_http_mysql", db_details)
        self.validate_database_creation("test_http_mysql")

    def test_sql_create_predictor(self, train_finetune_lock):
        self.sql_via_http("USE mindsdb;", RESPONSE_TYPE.OK)
        self.sql_via_http("DROP MODEL IF EXISTS p_test_http_1;", RESPONSE_TYPE.OK)

        with train_finetune_lock.acquire(timeout=600):
            self.sql_via_http('''
                create predictor p_test_http_1
                from test_http_postgres (select sqft, location, rental_price from demo_data.home_rentals limit 30)
                predict rental_price
            ''', RESPONSE_TYPE.TABLE)
            status = self.await_model('p_test_http_1', timeout=120)
        assert status == 'complete'

        resp = self.sql_via_http('''
            select * from mindsdb.p_test_http_1 where sqft = 1000
        ''', RESPONSE_TYPE.TABLE)
        sqft_index = resp['column_names'].index('sqft')
        rental_price_index = resp['column_names'].index('rental_price')
        assert len(resp['data']) == 1
        assert resp['data'][0][sqft_index] == 1000
        assert resp['data'][0][rental_price_index] > 0

    def test_list_projects(self):
        project_name = 'mindsdb'
        response = self.api_request('get', '/projects')
        assert response.status_code == 200, 'Error to get list of projects'

        projects = [i['name'] for i in response.json()]
        assert project_name in projects

    def test_list_models(self):
        project_name = 'mindsdb'
        model_name = 'p_test_http_1'
        response = self.api_request('get', f'/projects/{project_name}/models')
        assert response.status_code == 200, 'Error to get list of models'
        models = [i['name'] for i in response.json()]
        assert model_name in models

    def test_make_prediction(self):
        project_name = 'mindsdb'
        model_name = 'p_test_http_1'
        payload = {
            'data': [{'sqft': '1000'},
                     {'sqft': '500'}]
        }
        response = self.api_request('post', f'/projects/{project_name}/models/{model_name}/predict', payload=payload)
        assert response.status_code == 200, 'Error to make prediction'

        # 2 prediction result
        assert len(response.json()) == 2

        # 1st version of model
        response = self.api_request('post', f'/projects/{project_name}/models/{model_name}.1/predict', payload=payload)
        assert response.status_code == 200, 'Error to make prediction'

        assert len(response.json()) == 2

    def test_tabs(self):
        COMPANY_1_ID = 9999998
        COMPANY_2_ID = 9999999

        def tabs_requets(method: str, url: str = '', payload: dict = {},
                         company_id: int = 1, expected_status: int = 200):
            resp = self.api_request(method, f'/tabs/{url}', payload=payload, headers={'company-id': str(company_id)})
            assert resp.status_code == expected_status
            return resp

        def compare_tabs(ta: dict, tb: dict) -> bool:
            for key in ('id', 'index', 'name', 'content'):
                if ta.get(key) != tb.get(key):
                    return False
            return True

        def compare_tabs_list(list_a: List[dict], list_b: List[dict]) -> bool:
            if len(list_a) != len(list_b):
                return False
            for i in range(len(list_a)):
                if compare_tabs(list_a[i], list_b[i]) is False:
                    return False
            return True

        def tab(company_id: int, tab_number: int):
            return {
                'name': f'tab_name_{company_id}_{tab_number}',
                'content': f'tab_content_{company_id}_{tab_number}'
            }

        # users has empty tabs list
        for company_id in (COMPANY_1_ID, COMPANY_2_ID):
            resp = tabs_requets('get', '?mode=new', company_id=company_id)
            # Delete all tabs to begin with
            for t in resp.json():
                tabs_requets('delete', str(t['id']), company_id=company_id)
            # Check that all tabs are deleted
            resp = tabs_requets('get', company_id=company_id)
            assert len(resp.json()) == 0

        # add tab and check fields
        tab_1_1 = tab(COMPANY_1_ID, 1)
        tabs_requets('post', '?mode=new', payload=tab_1_1, company_id=COMPANY_1_ID)
        resp_list = tabs_requets('get', '?mode=new', company_id=COMPANY_1_ID).json()
        assert len(resp_list) == 1
        resp_1_1 = resp_list[0]
        assert resp_1_1['name'] == tab_1_1['name']
        assert resp_1_1['content'] == tab_1_1['content']
        assert isinstance(resp_1_1['id'], int)
        assert isinstance(resp_1_1['index'], int)
        tab_1_1['id'] = resp_1_1['id']
        tab_1_1['index'] = resp_1_1['index']

        # second list is empty
        resp = tabs_requets('get', '?mode=new', company_id=COMPANY_2_ID).json()
        assert len(resp) == 0

        # add tab to second user
        tab_2_1 = tab(COMPANY_2_ID, 1)
        tabs_requets('post', '?mode=new', payload=tab_2_1, company_id=COMPANY_2_ID)
        resp_list = tabs_requets('get', '?mode=new', company_id=COMPANY_2_ID).json()
        assert len(resp_list) == 1
        resp_2_1 = resp_list[0]
        assert resp_2_1['name'] == tab_2_1['name']
        assert resp_2_1['content'] == tab_2_1['content']
        tab_2_1['id'] = resp_2_1['id']
        tab_2_1['index'] = resp_2_1['index']

        # add few tabs for tests
        tab_1_2 = tab(COMPANY_1_ID, 2)
        tab_2_2 = tab(COMPANY_2_ID, 2)
        for tab_dict, company_id in ((tab_1_2, COMPANY_1_ID), (tab_2_2, COMPANY_2_ID)):
            tab_meta = tabs_requets('post', '?mode=new', payload=tab_dict, company_id=company_id).json()['tab_meta']
            tab_dict['id'] = tab_meta['id']
            tab_dict['index'] = tab_meta['index']

        resp_list = tabs_requets('get', '?mode=new', company_id=COMPANY_1_ID).json()
        assert compare_tabs_list(resp_list, [tab_1_1, tab_1_2])

        resp_list = tabs_requets('get', '?mode=new', company_id=COMPANY_2_ID).json()
        assert compare_tabs_list(resp_list, [tab_2_1, tab_2_2])

        # add tab to second index
        tab_1_3 = tab(COMPANY_1_ID, 3)
        tab_1_3['index'] = tab_1_1['index'] + 1
        tab_meta = tabs_requets('post', '?mode=new', payload=tab_1_3, company_id=COMPANY_1_ID).json()['tab_meta']
        tab_1_3['id'] = tab_meta['id']
        tabs_list = tabs_requets('get', '?mode=new', company_id=COMPANY_1_ID).json()
        assert len(tabs_list) == 3
        tab_1_1['index'] = tabs_list[0]['index']
        tab_1_3['index'] = tabs_list[1]['index']
        tab_1_2['index'] = tabs_list[2]['index']
        assert compare_tabs_list(tabs_list, [tab_1_1, tab_1_3, tab_1_2])
        assert tab_1_1['index'] < tab_1_3['index'] < tab_1_2['index']

        # update tab content and index
        tab_1_2['index'] = tab_1_1['index'] + 1
        tab_1_2['content'] = tab_1_2['content'] + '_new'
        tab_meta = tabs_requets(
            'put',
            str(tab_1_2['id']),
            payload={'index': tab_1_2['index'], 'content': tab_1_2['content']},
            company_id=COMPANY_1_ID
        ).json()['tab_meta']
        assert tab_meta['index'] == tab_1_2['index']
        assert tab_meta['name'] == tab_1_2['name']
        assert tab_meta['id'] == tab_1_2['id']
        tabs_list = tabs_requets('get', '?mode=new', company_id=COMPANY_1_ID).json()
        tab_1_3['index'] = tab_1_2['index'] + 1
        assert compare_tabs_list(tabs_list, [tab_1_1, tab_1_2, tab_1_3])

        # update tab content and name
        tab_1_2['content'] = tab_1_2['content'] + '_new'
        tab_1_2['name'] = tab_1_2['name'] + '_new'
        tabs_requets('put', str(tab_1_2['id']),
                     payload={'name': tab_1_2['name'], 'content': tab_1_2['content']}, company_id=COMPANY_1_ID)
        tabs_list = tabs_requets('get', '?mode=new', company_id=COMPANY_1_ID).json()
        assert compare_tabs_list(tabs_list, [tab_1_1, tab_1_2, tab_1_3])

        # second list does not changed
        tabs_list = tabs_requets('get', '?mode=new', company_id=COMPANY_2_ID).json()
        assert compare_tabs_list(tabs_list, [tab_2_1, tab_2_2])

        # get each tab one by one
        for company_id, tabs in ((COMPANY_1_ID, [tab_1_1, tab_1_2, tab_1_3]), (COMPANY_2_ID, [tab_2_1, tab_2_2])):
            for tab_dict in tabs:
                tab_resp = tabs_requets('get', str(tab_dict['id']), company_id=company_id).json()
                assert compare_tabs(tab_resp, tab_dict)

        # check failures
        tabs_requets('get', '99', company_id=COMPANY_1_ID, expected_status=404)
        tabs_requets('delete', '99', company_id=COMPANY_1_ID, expected_status=404)
        tabs_requets('post', '?mode=new', payload={'whaaat': '?', 'name': 'test'}, company_id=COMPANY_1_ID, expected_status=400)
        tabs_requets('put', '99', payload={'name': 'test'}, company_id=COMPANY_1_ID, expected_status=404)
        tabs_requets('put', str(tab_1_1['id']), payload={'whaaat': '?'}, company_id=COMPANY_1_ID, expected_status=400)
