import requests
import time

from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from .conftest import HTTP_API_ROOT


class HTTPHelperMixin:
    _sql_via_http_context = {}

    @staticmethod
    def api_request(method, url, payload=None, headers=None):
        method = method.lower()

        fnc = getattr(requests, method)

        url = f'{HTTP_API_ROOT}/{url.lstrip("/")}'
        response = fnc(url, json=payload, headers=headers)

        return response

    def sql_via_http(self, request: str, expected_resp_type: str = None, context: dict = None,
                     headers: dict = None, company_id: int = None) -> dict:
        if context is None:
            context = self._sql_via_http_context

        if headers is None:
            headers = {}
        if company_id is not None:
            headers['company-id'] = str(company_id)

        payload = {
            'query': request,
            'context': context
        }
        response = self.api_request('post', '/sql/query', payload, headers)

        assert response.status_code == 200, f"sql/query is not accessible - {response.text}"
        response = response.json()
        if expected_resp_type is not None:
            assert response.get('type') == expected_resp_type, response
        else:
            assert response.get('type') in [RESPONSE_TYPE.OK, RESPONSE_TYPE.TABLE, RESPONSE_TYPE.ERROR], response
        assert isinstance(response.get('context'), dict)
        if response['type'] == 'table':
            assert isinstance(response.get('data'), list)
            assert isinstance(response.get('column_names'), list)
        elif response['type'] == 'error':
            assert isinstance(response.get('error_code'), int)
            assert isinstance(response.get('error_message'), str)
        self._sql_via_http_context = response['context']
        return response

    def await_model(self, model_name: str, project_name: str = 'mindsdb',
                    version_number: int = 1, timeout: int = 60):
        start = time.time()
        status = None
        while (time.time() - start) < timeout:
            response = self.sql_via_http(
                f"""
                    SELECT status
                    FROM {project_name}.models
                    WHERE name='{model_name}' and version = {version_number}
                """, RESPONSE_TYPE.TABLE
            )
            status = response['data'][0][0]
            if status in ['complete', 'error']:
                break
            time.sleep(1)
        return status

    def await_model_by_query(self, query, timeout=60):
        start = time.time()
        status = None
        while (time.time() - start) < timeout:
            resp = self.sql_via_http(query, RESPONSE_TYPE.TABLE)
            status_index = [x.lower() for x in resp['column_names']].index('status')
            status = resp['data'][0][status_index]
            if status in ['complete', 'error']:
                break
            time.sleep(1)
        return status


def get_predictors_list(company_id=None):
    headers = {}
    if company_id is not None:
        headers['company-id'] = f'{company_id}'
    res = requests.get(f'{HTTP_API_ROOT}/predictors/', headers=headers)
    assert res.status_code == 200
    return res.json()


def get_predictors_names_list(company_id=None):
    predictors = get_predictors_list(company_id=company_id)
    return [x['name'] for x in predictors]


def check_predictor_exists(name):
    assert name in get_predictors_names_list()


def check_predictor_not_exists(name):
    assert name not in get_predictors_names_list()


def get_predictor_data(name):
    predictors = get_predictors_list()
    for p in predictors:
        if p['name'] == name:
            return p
    return None


def wait_predictor_learn(predictor_name):
    start_time = time.time()
    learn_done = False
    while learn_done is False and (time.time() - start_time) < 180:
        learn_done = get_predictor_data(predictor_name)['status'] == 'complete'
        time.sleep(1)
    assert learn_done


def get_integrations_names(company_id=None):
    headers = {}
    if company_id is not None:
        headers['company-id'] = f'{company_id}'
    res = requests.get(f'{HTTP_API_ROOT}/config/integrations', headers=headers)
    assert res.status_code == 200
    return res.json()['integrations']
