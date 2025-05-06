import json

from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from tests.utils.http_test_helpers import HTTPHelperMixin

CID_A = 19999996
CID_B = 29999996


class TestCompanyIndependent(HTTPHelperMixin):

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

    def test_add_data_db_http(self):
        self.sql_via_http("CREATE PROJECT mindsdb;", company_id=CID_A)
        self.sql_via_http("CREATE PROJECT mindsdb;", company_id=CID_B)
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

        self.sql_via_http("DROP DATABASE IF EXISTS test_integration_a", company_id=CID_A,
                          expected_resp_type=RESPONSE_TYPE.OK)
        self.sql_via_http("DROP DATABASE IF EXISTS test_integration_b", company_id=CID_B,
                          expected_resp_type=RESPONSE_TYPE.OK)

        self.sql_via_http(
            f"""
                CREATE DATABASE test_integration_a
                ENGINE '{db_details['type']}'
                PARAMETERS {json.dumps(db_details['connection_data'])}
            """,
            company_id=CID_A,
            expected_resp_type=RESPONSE_TYPE.OK
        )

        databases_names_a = self.get_db_names(CID_A)
        self.assert_list(
            databases_names_a, {
                'information_schema',
                'log',
                'mindsdb',
                'test_integration_a'
            }
        )

        databases_names_b = self.get_db_names(CID_B)
        self.assert_list(
            databases_names_b, {
                'information_schema',
                'log',
                'mindsdb'
            }
        )

        self.sql_via_http("DROP DATABASE IF EXISTS test_integration_b", company_id=CID_A,
                          expected_resp_type=RESPONSE_TYPE.OK)
        self.sql_via_http(
            f"""
                CREATE DATABASE test_integration_b
                ENGINE '{db_details['type']}'
                PARAMETERS {json.dumps(db_details['connection_data'])}
            """,
            company_id=CID_B,
            expected_resp_type=RESPONSE_TYPE.OK
        )

        databases_names_a = self.get_db_names(CID_A)
        self.assert_list(
            databases_names_a, {
                'information_schema',
                'log',
                'mindsdb',
                'test_integration_a'
            }
        )

        databases_names_b = self.get_db_names(CID_B)
        self.assert_list(
            databases_names_b, {
                'information_schema',
                'log',
                'mindsdb',
                'test_integration_b'
            }
        )

        self.sql_via_http(
            "DROP DATABASE test_integration_a",
            company_id=CID_A,
            expected_resp_type=RESPONSE_TYPE.OK
        )

        databases_names_a = self.get_db_names(CID_A)
        self.assert_list(
            databases_names_a, {
                'information_schema',
                'log',
                'mindsdb'
            }
        )

        databases_names_b = self.get_db_names(CID_B)
        self.assert_list(
            databases_names_b, {
                'information_schema',
                'log',
                'mindsdb',
                'test_integration_b'
            }
        )

        self.sql_via_http(
            f"""
                CREATE DATABASE test_integration_a
                ENGINE '{db_details['type']}'
                PARAMETERS {json.dumps(db_details['connection_data'])}
            """,
            company_id=CID_A,
            expected_resp_type=RESPONSE_TYPE.OK
        )

        databases_names_a = self.get_db_names(CID_A)
        self.assert_list(
            databases_names_a, {
                'information_schema',
                'log',
                'mindsdb',
                'test_integration_a'
            }
        )

        databases_names_b = self.get_db_names(CID_B)
        self.assert_list(
            databases_names_b, {
                'information_schema',
                'log',
                'mindsdb',
                'test_integration_b'
            }
        )

        response = self.sql_via_http(
            "select * from test_integration_a.demo_data.home_rentals limit 10",
            company_id=CID_A,
            expected_resp_type=RESPONSE_TYPE.TABLE
        )
        assert len(response['data']) == 10

        response = self.sql_via_http(
            "select * from test_integration_a.demo_data.home_rentals limit 10",
            company_id=CID_B,
            expected_resp_type=RESPONSE_TYPE.ERROR
        )

    def test_add_ml_engine(self):

        for cid in [CID_A, CID_B]:
            self.sql_via_http(
                "DROP ML_ENGINE IF EXISTS test_comp_ml", company_id=cid,
                expected_resp_type=RESPONSE_TYPE.OK)
            self.sql_via_http(
                "CREATE ML_ENGINE test_comp_ml FROM lightwood USING password=''",
                company_id=cid,
                expected_resp_type=RESPONSE_TYPE.OK
            )

            assert "test_comp_ml" in self.get_ml_engines(cid)

    def test_views(self):

        query = """
            CREATE VIEW {}
            FROM test_integration_{} (
                select * from demo_data.home_rentals limit 50
            )
        """

        for cid, char in [(CID_A, 'a'), (CID_B, 'b')]:
            self.sql_via_http(f"DROP VIEW IF EXISTS test_view_{char}", company_id=cid)
            self.sql_via_http(f"DROP MODEL IF EXISTS model_{char}", company_id=cid)
            self.sql_via_http(
                query.format(f'test_view_{char}', char),
                company_id=cid,
                expected_resp_type=RESPONSE_TYPE.OK
            )

            tables = self.get_tables_in('mindsdb', cid)
            self.assert_list(
                tables, {
                    'models',
                    f'test_view_{char}'
                }
            )

        for cid, char in [(CID_A, 'a'), (CID_B, 'b')]:
            response = self.sql_via_http(
                f"select * from test_view_{char}",
                company_id=cid,
                expected_resp_type=RESPONSE_TYPE.TABLE
            )
            assert len(response['data']) == 50

            response = self.sql_via_http(
                f"DROP VIEW test_view_{char}",
                company_id=cid,
                expected_resp_type=RESPONSE_TYPE.OK
            )

            tables = self.get_tables_in('mindsdb', cid)
            self.assert_list(
                tables, {
                    'models',
                }
            )

            self.sql_via_http(
                f"select * from test_view_{char}",
                company_id=cid,
                expected_resp_type=RESPONSE_TYPE.ERROR
            )

    def test_model(self, train_finetune_lock):
        query = """
            CREATE MODEL model_{}
            FROM test_integration_{} (
                select * from demo_data.home_rentals limit 50
            ) PREDICT rental_price
            USING join_learn_process=true, time_aim=5
        """

        predict_query = """
            select * from model_{} where sqft = 100
        """

        for cid, char in [(CID_A, 'a'), (CID_B, 'b')]:
            with train_finetune_lock.acquire(timeout=600):
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
