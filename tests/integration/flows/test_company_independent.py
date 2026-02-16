import json
import pytest

from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from tests.integration.utils.http_test_helpers import HTTPHelperMixin
from tests.integration.conftest import get_test_company_id, get_test_user_id, get_test_resource_name, create_byom

# Use unique company/user IDs to avoid conflicts between test runs
CID_A = get_test_company_id(1)
CID_B = get_test_company_id(2)
USER_ID_A = get_test_user_id(1)
USER_ID_B = get_test_user_id(2)

# Unique resource names
DB_NAME_A = get_test_resource_name("test_integration_a")
DB_NAME_B = get_test_resource_name("test_integration_b")
ML_ENGINE_NAME = get_test_resource_name("test_comp_ml")
VIEW_NAME_A = get_test_resource_name("test_view_a")
VIEW_NAME_B = get_test_resource_name("test_view_b")
MODEL_NAME_A = get_test_resource_name("model_a")
MODEL_NAME_B = get_test_resource_name("model_b")


class TestCompanyIndependent(HTTPHelperMixin):
    def get_db_names(self, company_id: str, user_id: str):
        response = self.sql_via_http(
            "show databases", company_id=company_id, user_id=user_id, expected_resp_type=RESPONSE_TYPE.TABLE
        )
        return [x[0].lower() for x in response["data"]]

    def get_tables_in(self, table, company_id: str, user_id: str):
        response = self.sql_via_http(
            f"SHOW TABLES FROM {table}", company_id=company_id, user_id=user_id, expected_resp_type=RESPONSE_TYPE.TABLE
        )
        return [x[0].lower() for x in response["data"]]

    def get_ml_engines(self, company_id: str, user_id: str):
        response = self.sql_via_http(
            "SHOW ML_ENGINES", company_id=company_id, user_id=user_id, expected_resp_type=RESPONSE_TYPE.TABLE
        )
        return [x[0].lower() for x in response["data"]]

    def assert_list(self, a, b):
        a = set(a)
        b = set(b)
        assert len(a) == len(b)
        assert a == b

    def test_add_data_db_http(self):
        self.sql_via_http("CREATE PROJECT IF NOT EXISTS mindsdb;", company_id=CID_A, user_id=USER_ID_A)
        self.sql_via_http("CREATE PROJECT IF NOT EXISTS mindsdb;", company_id=CID_B, user_id=USER_ID_B)
        db_details = {
            "type": "postgres",
            "connection_data": {
                "type": "postgres",
                "host": "samples.mindsdb.com",
                "port": "5432",
                "user": "demo_user",
                "password": "demo_password",
                "database": "demo",
            },
        }

        self.sql_via_http(
            f"DROP DATABASE IF EXISTS {DB_NAME_A}",
            company_id=CID_A,
            user_id=USER_ID_A,
            expected_resp_type=RESPONSE_TYPE.OK,
        )
        self.sql_via_http(
            f"DROP DATABASE IF EXISTS {DB_NAME_B}",
            company_id=CID_B,
            user_id=USER_ID_B,
            expected_resp_type=RESPONSE_TYPE.OK,
        )

        self.sql_via_http(
            f"""
                CREATE DATABASE {DB_NAME_A}
                ENGINE '{db_details["type"]}'
                PARAMETERS {json.dumps(db_details["connection_data"])}
            """,
            company_id=CID_A,
            user_id=USER_ID_A,
            expected_resp_type=RESPONSE_TYPE.OK,
        )

        databases_names_a = self.get_db_names(CID_A, USER_ID_A)
        assert "information_schema" in databases_names_a
        assert "mindsdb" in databases_names_a
        assert DB_NAME_A.lower() in databases_names_a

        databases_names_b = self.get_db_names(CID_B, USER_ID_B)
        assert "information_schema" in databases_names_b
        assert "mindsdb" in databases_names_b

        self.sql_via_http(
            f"DROP DATABASE IF EXISTS {DB_NAME_B}",
            company_id=CID_A,
            user_id=USER_ID_A,
            expected_resp_type=RESPONSE_TYPE.OK,
        )
        self.sql_via_http(
            f"""
                CREATE DATABASE {DB_NAME_B}
                ENGINE '{db_details["type"]}'
                PARAMETERS {json.dumps(db_details["connection_data"])}
            """,
            company_id=CID_B,
            user_id=USER_ID_B,
            expected_resp_type=RESPONSE_TYPE.OK,
        )

        databases_names_a = self.get_db_names(CID_A, USER_ID_A)
        assert DB_NAME_A.lower() in databases_names_a

        databases_names_b = self.get_db_names(CID_B, USER_ID_B)
        assert DB_NAME_B.lower() in databases_names_b

        self.sql_via_http(
            f"DROP DATABASE {DB_NAME_A}", company_id=CID_A, user_id=USER_ID_A, expected_resp_type=RESPONSE_TYPE.OK
        )

        databases_names_a = self.get_db_names(CID_A, USER_ID_A)
        assert DB_NAME_A.lower() not in databases_names_a

        databases_names_b = self.get_db_names(CID_B, USER_ID_B)
        assert DB_NAME_B.lower() in databases_names_b

        self.sql_via_http(
            f"""
                CREATE DATABASE {DB_NAME_A}
                ENGINE '{db_details["type"]}'
                PARAMETERS {json.dumps(db_details["connection_data"])}
            """,
            company_id=CID_A,
            user_id=USER_ID_A,
            expected_resp_type=RESPONSE_TYPE.OK,
        )

        databases_names_a = self.get_db_names(CID_A, USER_ID_A)
        assert DB_NAME_A.lower() in databases_names_a

        databases_names_b = self.get_db_names(CID_B, USER_ID_B)
        assert DB_NAME_B.lower() in databases_names_b

        response = self.sql_via_http(
            f"select * from {DB_NAME_A}.demo_data.home_rentals limit 10",
            company_id=CID_A,
            user_id=USER_ID_A,
            expected_resp_type=RESPONSE_TYPE.TABLE,
        )
        assert len(response["data"]) == 10

        response = self.sql_via_http(
            f"select * from {DB_NAME_A}.demo_data.home_rentals limit 10",
            company_id=CID_B,
            user_id=USER_ID_B,
            expected_resp_type=RESPONSE_TYPE.ERROR,
        )

    @pytest.mark.skip(reason="Requires ML handler (lightwood removed)")
    def test_add_ml_engine(self):
        tracker = self.get_resource_tracker()

        for cid, user_id in [(CID_A, USER_ID_A), (CID_B, USER_ID_B)]:
            self.sql_via_http(
                f"DROP ML_ENGINE IF EXISTS {ML_ENGINE_NAME}",
                company_id=cid,
                user_id=user_id,
                expected_resp_type=RESPONSE_TYPE.OK,
            )
            self.sql_via_http(
                f"CREATE ML_ENGINE {ML_ENGINE_NAME} FROM test_ml_engine",
                company_id=cid,
                user_id=user_id,
                expected_resp_type=RESPONSE_TYPE.OK,
            )
            tracker.track_ml_engine(ML_ENGINE_NAME)

            assert ML_ENGINE_NAME.lower() in self.get_ml_engines(cid, user_id)

    def test_views(self):
        tracker = self.get_resource_tracker()

        # Map of (company_id, user_id) -> (db_name, view_name, model_name)
        test_configs = [
            (CID_A, USER_ID_A, DB_NAME_A, VIEW_NAME_A, MODEL_NAME_A),
            (CID_B, USER_ID_B, DB_NAME_B, VIEW_NAME_B, MODEL_NAME_B),
        ]

        for cid, user_id, db_name, view_name, model_name in test_configs:
            self.sql_via_http(f"DROP VIEW IF EXISTS {view_name}", company_id=cid, user_id=user_id)
            self.sql_via_http(f"DROP MODEL IF EXISTS {model_name}", company_id=cid, user_id=user_id)
            self.sql_via_http(
                f"""
                    CREATE VIEW {view_name}
                    FROM {db_name} (
                        select * from demo_data.home_rentals limit 50
                    )
                """,
                company_id=cid,
                user_id=user_id,
                expected_resp_type=RESPONSE_TYPE.OK,
            )
            tracker.track_view(view_name)

            tables = self.get_tables_in("mindsdb", cid, user_id)
            assert "models" in tables
            assert view_name.lower() in tables

        for cid, user_id, db_name, view_name, model_name in test_configs:
            response = self.sql_via_http(
                f"select * from {view_name}",
                company_id=cid,
                user_id=user_id,
                expected_resp_type=RESPONSE_TYPE.TABLE,
            )
            assert len(response["data"]) == 50

            response = self.sql_via_http(
                f"DROP VIEW {view_name}", company_id=cid, user_id=user_id, expected_resp_type=RESPONSE_TYPE.OK
            )

            tables = self.get_tables_in("mindsdb", cid, user_id)
            assert "models" in tables
            assert view_name.lower() not in tables

            self.sql_via_http(
                f"select * from {view_name}",
                company_id=cid,
                user_id=user_id,
                expected_resp_type=RESPONSE_TYPE.ERROR,
            )

    @pytest.mark.skip(
        reason="Disabled after deleting lightwood. No suitable handler available and BYOM usage restricted."
    )
    def test_model(self, train_finetune_lock):
        tracker = self.get_resource_tracker()

        # Map of (company_id, user_id) -> (db_name, model_name)
        test_configs = [
            (CID_A, USER_ID_A, DB_NAME_A, MODEL_NAME_A),
            (CID_B, USER_ID_B, DB_NAME_B, MODEL_NAME_B),
        ]

        for cid, user_id, db_name, model_name in test_configs:
            create_byom("test_ml_engine", target_column="rental_price", company_id=cid, user_id=user_id)
            with train_finetune_lock.acquire(timeout=600):
                self.sql_via_http(
                    f"""
                        CREATE MODEL {model_name}
                        FROM {db_name} (
                            select * from demo_data.home_rentals limit 50
                        ) PREDICT rental_price
                        USING engine='test_ml_engine', join_learn_process=true
                    """,
                    company_id=cid,
                    user_id=user_id,
                    expected_resp_type=RESPONSE_TYPE.TABLE,
                )
                tracker.track_model(model_name)

            response = self.sql_via_http(
                f"select * from {model_name} where sqft = 100",
                company_id=cid,
                user_id=user_id,
                expected_resp_type=RESPONSE_TYPE.TABLE,
            )
            assert len(response["data"]), 1
