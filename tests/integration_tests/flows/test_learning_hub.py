from pathlib import Path
import json
import time

import requests
import pytest

from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from .conftest import CONFIG_PATH

API_LIST = ["http", ]

HTTP_API_ROOT = f'http://127.0.0.1:47334/api'


@pytest.mark.usefixtures("mindsdb_app")
class TestLearningHub:

    @classmethod
    def setup_class(cls):
        cls.config = json.loads(
            Path(CONFIG_PATH).read_text()
        )

        cls.initial_integrations_names = list(cls.config['integrations'].keys())
        cls._sql_via_http_context = {}

    def api_request(self, method, url, payload=None):
        method = method.lower()

        fnc = getattr(requests, method)

        url = f'{HTTP_API_ROOT}/{url.lstrip("/")}'
        response = fnc(url, json=payload)

        return response

    def sql_via_http(self, request: str, expected_resp_type: str = None, context: dict = None):
        if context is None:
            context = self._sql_via_http_context

        payload = {
            'query': request,
            'context': context
        }

        response = self.api_request('post', f'/sql/query', payload)

        assert response.status_code == 200, f"sql/query is not accessible - {response.text}"
        response = response.json()

        assert response.get('type') == (expected_resp_type or [RESPONSE_TYPE.OK, RESPONSE_TYPE.TABLE, RESPONSE_TYPE.ERROR])
        assert isinstance(response.get('context'), dict)

        if response['type'] == 'table':
            assert isinstance(response.get('data'), list)
            assert isinstance(response.get('column_names'), list)
        elif response['type'] == 'error':
            assert isinstance(response.get('error_code'), int)
            assert isinstance(response.get('error_message'), str)

        self._sql_via_http_context = response['context']

        return response

    def await_predictor(self, predictor_name, timeout=60):
        start = time.time()
        status = None
        while (time.time() - start) < timeout:
            resp = self.sql_via_http('show models', RESPONSE_TYPE.TABLE)
            name_index = [x.lower() for x in resp['column_names']].index('name')
            status_index = [x.lower() for x in resp['column_names']].index('status')
            for row in resp['data']:
                if row[name_index] == predictor_name:
                    status = row[status_index]
            if status in ['complete', 'error']:
                break
            time.sleep(1)
        return status

    def show_databases(self):
        resp = self.sql_via_http('show databases', RESPONSE_TYPE.TABLE)
        return [x[0] for x in resp['data']]

    def test_predict_home_rentals(self):
        self.sql_via_http('''
          CREATE DATABASE example_db
          WITH ENGINE = "postgres",
          PARAMETERS = {
              "user": "demo_user",
              "password": "demo_password",
              "host": "3.220.66.106",
              "port": "5432",
              "database": "demo"
              };
        ''', RESPONSE_TYPE.OK)
        assert 'example_db' in self.show_databases()

        resp = self.sql_via_http('''
          SELECT * 
          FROM example_db.demo_data.home_rentals 
          LIMIT 10;
        ''', RESPONSE_TYPE.TABLE)
        assert len(resp['data']) > 0
        assert len(resp['column_names']) == 7

        print(resp['data'])
