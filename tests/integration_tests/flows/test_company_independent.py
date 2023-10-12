from pathlib import Path
import json

import requests
import pytest

from pymongo import MongoClient

from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from .conftest import CONFIG_PATH

# used by mindsdb_app fixture in conftest
OVERRIDE_CONFIG = {
    'integrations': {},
}

# used by (required for) mindsdb_app fixture in conftest
API_LIST = ["http", 'mongodb']

CONFIG = {}
CID_A = 1
CID_B = 2


def get_string_params(parameters):
    return ', '.join([f'{key} = {json.dumps(val)}' for key, val in parameters.items()])


@pytest.mark.usefixtures('mindsdb_app', 'postgres_db')
class TestCompanyIndependent:
    @classmethod
    def setup_class(cls):
        CONFIG.update(
            json.loads(
                Path(CONFIG_PATH).read_text()
            )
        )

    def get_db_names(self, company_id: int = None):
        response = self.sql_via_http(
            'show databases',
            company_id=company_id,
            expected_resp_type=RESPONSE_TYPE.TABLE
        )
        return [x[0].lower() for x in response['data']]

    def get_tables_in(self, table, company_id):
        response = self.sql_via_http(
            f"SHOW TABLES FROM {table}",
            company_id=company_id,
            expected_resp_type=RESPONSE_TYPE.TABLE
        )
        return [x[0].lower() for x in response['data']]

    def get_ml_engines(self, company_id: int = None):
        response = self.sql_via_http(
            "SHOW ML_ENGINES",
            company_id=company_id,
            expected_resp_type=RESPONSE_TYPE.TABLE
        )
        return [x[0].lower() for x in response['data']]

    def assert_list(self, a, b):
        a = set(a)
        b = set(b)
        assert len(a) == len(b)
        assert a == b

    def sql_via_http(self, request: str, expected_resp_type: str = None, context: dict = None,
                     headers: dict = None, company_id: int = None) -> dict:
        if context is None:
            context = {}

        if headers is None:
            headers = {}
        if company_id is not None:
            headers['company-id'] = str(company_id)

        root = 'http://127.0.0.1:47334/api'
        response = requests.post(
            f'{root}/sql/query',
            json={
                'query': request,
                'context': context
            },
            headers=headers
        )
        assert response.status_code == 200
        response = response.json()
        if expected_resp_type is not None:
            assert response.get('type') == expected_resp_type
        else:
            assert response.get('type') in [RESPONSE_TYPE.OK, RESPONSE_TYPE.TABLE, RESPONSE_TYPE.ERROR]
        assert isinstance(response.get('context'), dict)
        if response['type'] == 'table':
            assert isinstance(response.get('data'), list)
            assert isinstance(response.get('column_names'), list)
        elif response['type'] == 'error':
            assert isinstance(response.get('error_code'), int)
            assert isinstance(response.get('error_message'), str)
        self._sql_via_http_context = response['context']
        return response

    def test_initial_state_http(self):
        # add permanent integrations
        for cid in [CID_A, CID_B]:
            databases_names = self.get_db_names(cid)
            assert len(databases_names) == 1 and databases_names[0] == 'information_schema'
            self.sql_via_http(
                "CREATE DATABASE files ENGINE='files'",
                company_id=cid,
                expected_resp_type=RESPONSE_TYPE.OK
            )
            databases_names = self.get_db_names(cid)
            self.assert_list(
                databases_names, {
                    'information_schema',
                    'files'
                }
            )
            self.sql_via_http(
                'CREATE DATABASE mindsdb',
                company_id=cid,
                expected_resp_type=RESPONSE_TYPE.OK
            )
            databases_names = self.get_db_names(cid)
            self.assert_list(
                databases_names, {
                    'information_schema',
                    'mindsdb',
                    'files'
                }
            )

    def test_add_data_db_http(self):

        # region create data db
        test_integration_data = self.postgres_db["connection_data"]
        test_integration_engine = self.postgres_db['type']

        self.sql_via_http(
            f"""
                CREATE DATABASE test_integration_a
                ENGINE '{test_integration_engine}'
                PARAMETERS {json.dumps(test_integration_data)}
            """,
            company_id=CID_A,
            expected_resp_type=RESPONSE_TYPE.OK
        )

        databases_names_a = self.get_db_names(CID_A)
        self.assert_list(
            databases_names_a, {
                'information_schema',
                'mindsdb',
                'files',
                'test_integration_a'
            }
        )

        databases_names_b = self.get_db_names(CID_B)
        self.assert_list(
            databases_names_b, {
                'information_schema',
                'mindsdb',
                'files'
            }
        )

        self.sql_via_http(
            f"""
                CREATE DATABASE test_integration_b
                ENGINE '{test_integration_engine}'
                PARAMETERS {json.dumps(test_integration_data)}
            """,
            company_id=CID_B,
            expected_resp_type=RESPONSE_TYPE.OK
        )

        databases_names_a = self.get_db_names(CID_A)
        self.assert_list(
            databases_names_a, {
                'information_schema',
                'mindsdb',
                'files',
                'test_integration_a'
            }
        )

        databases_names_b = self.get_db_names(CID_B)
        self.assert_list(
            databases_names_b, {
                'information_schema',
                'mindsdb',
                'files',
                'test_integration_b'
            }
        )
        # endregion

        # region del data bd and create again
        self.sql_via_http(
            "DROP DATABASE test_integration_a",
            company_id=CID_A,
            expected_resp_type=RESPONSE_TYPE.OK
        )

        databases_names_a = self.get_db_names(CID_A)
        self.assert_list(
            databases_names_a, {
                'information_schema',
                'mindsdb',
                'files'
            }
        )

        databases_names_b = self.get_db_names(CID_B)
        self.assert_list(
            databases_names_b, {
                'information_schema',
                'mindsdb',
                'files',
                'test_integration_b'
            }
        )

        self.sql_via_http(
            f"""
                CREATE DATABASE test_integration_a
                ENGINE '{test_integration_engine}'
                PARAMETERS {json.dumps(test_integration_data)}
            """,
            company_id=CID_A,
            expected_resp_type=RESPONSE_TYPE.OK
        )

        databases_names_a = self.get_db_names(CID_A)
        self.assert_list(
            databases_names_a, {
                'information_schema',
                'mindsdb',
                'files',
                'test_integration_a'
            }
        )

        databases_names_b = self.get_db_names(CID_B)
        self.assert_list(
            databases_names_b, {
                'information_schema',
                'mindsdb',
                'files',
                'test_integration_b'
            }
        )
        # endregion

        # region check tables
        for cid in [CID_A, CID_B]:
            tables = self.get_tables_in('mindsdb', cid)
            self.assert_list(
                tables, {
                    'jobs',
                    'jobs_history',
                    'models',
                    'models_versions',
                    'mdb_triggers',
                    'chatbots',
                }
            )
        # endregion

        # region cehck select from data db
        response = self.sql_via_http(
            "select * from test_integration_a.rentals limit 10",
            company_id=CID_A,
            expected_resp_type=RESPONSE_TYPE.TABLE
        )
        assert len(response['data']) == 10

        response = self.sql_via_http(
            "select * from test_integration_a.rentals limit 10",
            company_id=CID_B,
            expected_resp_type=RESPONSE_TYPE.ERROR
        )
        # endregion

    def test_add_ml_engine(self):

        for cid in [CID_A, CID_B]:
            engines = self.get_ml_engines(cid)
            assert len(engines) == 0

            self.sql_via_http(
                "CREATE ML_ENGINE lightwood FROM lightwood USING password=''",
                company_id=cid,
                expected_resp_type=RESPONSE_TYPE.OK
            )

            engines = self.get_ml_engines(cid)
            self.assert_list(
                engines, {
                    'lightwood'
                }
            )

    def test_views(self):

        query = """
            CREATE VIEW mindsdb.{}
            FROM test_integration_{} (
                select * from rentals limit 50
            )
        """

        for cid, char in [(CID_A, 'a'), (CID_B, 'b')]:
            self.sql_via_http(
                query.format(f'test_view_{char}', char),
                company_id=cid,
                expected_resp_type=RESPONSE_TYPE.OK
            )

            tables = self.get_tables_in('mindsdb', cid)
            self.assert_list(
                tables, {
                    'jobs',
                    'jobs_history',
                    'models',
                    'models_versions',
                    'mdb_triggers',
                    'chatbots',
                    f'test_view_{char}'
                }
            )

        for cid, char in [(CID_A, 'a'), (CID_B, 'b')]:
            response = self.sql_via_http(
                f"select * from mindsdb.test_view_{char}",
                company_id=cid,
                expected_resp_type=RESPONSE_TYPE.TABLE
            )
            assert len(response['data']) == 50

            response = self.sql_via_http(
                f"DROP VIEW mindsdb.test_view_{char}",
                company_id=cid,
                expected_resp_type=RESPONSE_TYPE.OK
            )

            tables = self.get_tables_in('mindsdb', cid)
            self.assert_list(
                tables, {
                    'jobs',
                    'jobs_history',
                    'models',
                    'models_versions',
                    'mdb_triggers',
                    'chatbots',
                }
            )

            self.sql_via_http(
                f"select * from mindsdb.test_view_{char}",
                company_id=cid,
                expected_resp_type=RESPONSE_TYPE.ERROR
            )

    def test_model(self):
        query = """
            CREATE MODEL mindsdb.model_{}
            FROM test_integration_{} (
                select * from rentals limit 50
            ) PREDICT rental_price
            USING join_learn_process=true, time_aim=5
        """

        predict_query = """
            select * from mindsdb.model_{} where sqft = 100
        """

        for cid, char in [(CID_A, 'a'), (CID_B, 'b')]:
            self.sql_via_http(
                query.format(char, char),
                company_id=cid,
                expected_resp_type=RESPONSE_TYPE.TABLE
            )
            response = self.sql_via_http(
                predict_query.format(char),
                company_id=cid,
                expected_resp_type=RESPONSE_TYPE.TABLE
            )
            assert len(response['data']), 1

    def test_6_mongo(self):

        client_a = MongoClient(host='127.0.0.1', port=int(CONFIG['api']['mongodb']['port']))
        client_a.admin.command({'company_id': CID_A, 'need_response': 1})

        client_b = MongoClient(host='127.0.0.1', port=int(CONFIG['api']['mongodb']['port']))
        client_b.admin.command({'company_id': CID_B, 'need_response': 1})

        databases = client_a.list_databases()
        self.assert_list([x['name'] for x in databases], {
            'admin', 'information_schema', 'mindsdb',
            'files', 'test_integration_a'
        })
        databases = client_b.list_databases()
        self.assert_list([x['name'] for x in databases], {
            'admin', 'information_schema', 'mindsdb',
            'files', 'test_integration_b'
        })

        client_a.mindsdb.models.insert_one({
            'name': 'test_mon_p_a',
            'predict': 'rental_price',
            'connection': 'test_integration_a',
            'select_data_query': 'select * from rentals limit 50',
            'training_options': {
                'join_learn_process': True,
                'time_aim': 3
            }
        })
        response = client_a.mindsdb.test_mon_p_a.find({
            'sqft': 100
        })
        assert len(list(response)) == 1

        collections = client_a.mindsdb.list_collection_names()
        self.assert_list(collections, {
            'jobs',
            'jobs_history',
            'models',
            'models_versions',
            'mdb_triggers',
            'chatbots',
            'test_mon_p_a',
            'model_a'
        })
        collections = client_b.mindsdb.list_collection_names()
        self.assert_list(collections, {
            'jobs',
            'jobs_history',
            'models',
            'models_versions',
            'mdb_triggers',
            'chatbots',
            'model_b'
        })
