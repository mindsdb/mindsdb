import time
import datetime
import random

import requests
import pytest

from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


# used by (required for) mindsdb_app fixture in conftest
API_LIST = ['http']

HTTP_API_ROOT = 'http://127.0.0.1:47334/api'


def to_dicts(data):
    data = [{
        'date': datetime.datetime.strptime(x[0].split(' ')[0], '%Y-%m-%d').date(),
        'group': x[1],
        'value': x[2]
    } for x in data]
    data.sort(key=lambda x: x['date'])
    return data


@pytest.mark.usefixtures("mindsdb_app")
class TestHTTP:
    @classmethod
    def setup_class(cls):
        cls._sql_via_http_context = {}

    def sql_via_http(self, request: str, expected_resp_type: str = None, context: dict = None) -> dict:
        if context is None:
            context = self._sql_via_http_context
        payload = {
            'query': request,
            'context': context
        }
        response = self.api_request('post', '/sql/query', payload)

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

    def api_request(self, method, url, payload=None, headers=None):
        method = method.lower()

        fnc = getattr(requests, method)

        url = f'{HTTP_API_ROOT}/{url.lstrip("/")}'
        response = fnc(url, json=payload, headers=headers)

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

    def test_create_model(self):
        sql = '''
        CREATE DATABASE example_db
        WITH ENGINE = "postgres",
        PARAMETERS = {
            "user": "demo_user",
            "password": "demo_password",
            "host": "3.220.66.106",
            "port": "5432",
            "database": "demo"
            };
        '''
        resp = self.sql_via_http(sql, RESPONSE_TYPE.OK)

        groups = ['a', 'b']
        selects = []
        for i in range(30):
            day_str = str(datetime.date.today() + datetime.timedelta(days=i))
            for group in groups:
                value = random.randint(0, 10)
                selects.append(f"select '{day_str}' as date, '{group}' as group, {value} as value")
        selects = ' union all '.join(selects)

        sql = f'''
            create view testv as (
                select * from example_db ({selects})
            )
        '''

        resp = self.sql_via_http(sql, RESPONSE_TYPE.OK)

        sql = '''
            CREATE MODEL
                mindsdb.tstest
            FROM mindsdb (select * from testv)
            PREDICT value
            ORDER BY date
            GROUP BY group
            WINDOW 5
            HORIZON 3;
        '''
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)

        assert len(resp['data']) == 1
        status = resp['column_names'].index('STATUS')
        assert resp['data'][0][status] == 'generating'

        self.await_predictor('tstest')

    def test_gt_latest_date(self):
        sql = '''
            select p.date, p.group, p.value
            from mindsdb.testv as t join mindsdb.tstest as p
            where t.date > LATEST
        '''
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)
        data = to_dicts(resp['data'])
        assert len(data) == 6
        assert len([x for x in data if x['group'] == 'a']) == 3
        assert data[0]['date'] == (datetime.date.today() + datetime.timedelta(days=30))

    def test_eq_latest_date(self):
        sql = '''
            select p.date, p.group, p.value
            from mindsdb.testv as t join mindsdb.tstest as p
            where t.date = LATEST
        '''
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)
        data = to_dicts(resp['data'])
        assert len(data) == 2
        assert len([x for x in data if x['group'] == 'a']) == 1
        assert data[0]['date'] == (datetime.date.today() + datetime.timedelta(days=29))

    def test_gt_particular_date(self):
        since = datetime.date.today() + datetime.timedelta(days=15)
        sql = f'''
            select p.date, p.group, p.value
            from mindsdb.testv as t join mindsdb.tstest as p
            where t.date > '{since}'
        '''
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)
        data = to_dicts(resp['data'])
        assert len(data) == 34  # 14 * 2 + 6 (4 days, 2 groups, 2*3 horizon)
        assert len([x for x in data if x['group'] == 'a']) == 17  # 14 + 3
        assert data[0]['date'] == (datetime.date.today() + datetime.timedelta(days=16))

    def test_eq_particular_date(self):
        since = datetime.date.today() + datetime.timedelta(days=15)
        sql = f'''
            select p.date, p.group, p.value
            from mindsdb.testv as t join mindsdb.tstest as p
            where t.date = '{since}'
        '''
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)
        data = to_dicts(resp['data'])
        assert len(data) == 2
        assert len([x for x in data if x['group'] == 'a']) == 1
        assert data[0]['date'] == (datetime.date.today() + datetime.timedelta(days=15))
