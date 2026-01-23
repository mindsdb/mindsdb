import datetime
import random

from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from tests.integration.utils.http_test_helpers import HTTPHelperMixin
from tests.integration.conftest import get_test_resource_name


def to_dicts(data):
    data = [
        {"date": datetime.datetime.strptime(x[0].split(" ")[0], "%Y-%m-%d").date(), "group": x[1], "value": x[2]}
        for x in data
    ]
    data.sort(key=lambda x: x["date"])
    return data


class TestHTTP(HTTPHelperMixin):
    # Unique resource names for this test session (initialized in setup_class)
    POSTGRES_DB_NAME = None
    VIEW_NAME = None
    MODEL_NAME = None

    @classmethod
    def setup_class(cls):
        cls._sql_via_http_context = {}
        # Initialize unique resource names for this test session
        cls.POSTGRES_DB_NAME = get_test_resource_name("test_ts_demo_postgres")
        cls.VIEW_NAME = get_test_resource_name("testv")
        cls.MODEL_NAME = get_test_resource_name("tstest")

    def test_create_model(self, train_finetune_lock):
        self.sql_via_http(f"DROP DATABASE IF EXISTS {self.POSTGRES_DB_NAME};", RESPONSE_TYPE.OK)
        sql = f"""
        CREATE DATABASE {self.POSTGRES_DB_NAME}
        WITH ENGINE = "postgres",
        PARAMETERS = {{
            "user": "demo_user",
            "password": "demo_password",
            "host": "samples.mindsdb.com",
            "port": "5432",
            "database": "demo"
            }};
        """
        resp = self.sql_via_http(sql, RESPONSE_TYPE.OK)

        groups = ["a", "b"]
        selects = []
        for i in range(30):
            day_str = str(datetime.date.today() + datetime.timedelta(days=i))
            for group in groups:
                value = random.randint(0, 10)
                selects.append(f"select '{day_str}' as date, '{group}' as group, {value} as value")
        selects = " union all ".join(selects)

        self.sql_via_http(f"DROP VIEW IF EXISTS {self.VIEW_NAME};", RESPONSE_TYPE.OK)
        sql = f"""
            create view {self.VIEW_NAME} as (
                select * from {self.POSTGRES_DB_NAME} ({selects})
            )
        """
        self.sql_via_http(sql, RESPONSE_TYPE.OK)

        self.sql_via_http(f"DROP MODEL IF EXISTS mindsdb.{self.MODEL_NAME};", RESPONSE_TYPE.OK)
        with train_finetune_lock.acquire(timeout=600):
            sql = f"""
                CREATE MODEL
                    mindsdb.{self.MODEL_NAME}
                FROM mindsdb (select * from {self.VIEW_NAME})
                PREDICT value
                ORDER BY date
                GROUP BY group
                WINDOW 5
                HORIZON 3;
            """
            resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)

            assert len(resp["data"]) == 1
            status = resp["column_names"].index("STATUS")
            assert resp["data"][0][status] == "generating"

            self.await_model(self.MODEL_NAME)

    def test_gt_latest_date(self):
        sql = f"""
            select p.date, p.group, p.value
            from mindsdb.{self.VIEW_NAME} as t join mindsdb.{self.MODEL_NAME} as p
            where t.date > LATEST
        """
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)
        data = to_dicts(resp["data"])
        assert len(data) == 6
        assert len([x for x in data if x["group"] == "a"]) == 3
        assert data[0]["date"] == (datetime.date.today() + datetime.timedelta(days=30))

    def test_gt_latest_date_empty_join(self):
        sql = f"""
            select p.date, p.group, p.value
            from mindsdb.{self.VIEW_NAME} as t join mindsdb.{self.MODEL_NAME} as p
            where t.date > LATEST and t.group = 'wrong'
        """
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)
        data = to_dicts(resp["data"])
        assert len(data) == 0

    def test_eq_latest_date(self):
        sql = f"""
            select p.date, p.group, p.value
            from mindsdb.{self.VIEW_NAME} as t join mindsdb.{self.MODEL_NAME} as p
            where t.date = LATEST
        """
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)
        data = to_dicts(resp["data"])
        assert len(data) == 2
        assert len([x for x in data if x["group"] == "a"]) == 1
        assert data[0]["date"] == (datetime.date.today() + datetime.timedelta(days=29))

    def test_gt_particular_date(self):
        since = datetime.date.today() + datetime.timedelta(days=15)
        sql = f"""
            select p.date, p.group, p.value
            from mindsdb.{self.VIEW_NAME} as t join mindsdb.{self.MODEL_NAME} as p
            where t.date > '{since}'
        """
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)
        data = to_dicts(resp["data"])
        assert len(data) == 34  # 14 * 2 + 6 (4 days, 2 groups, 2*3 horizon)
        assert len([x for x in data if x["group"] == "a"]) == 17  # 14 + 3
        assert data[0]["date"] == (datetime.date.today() + datetime.timedelta(days=16))

    def test_eq_particular_date(self):
        since = datetime.date.today() + datetime.timedelta(days=15)
        sql = f"""
            select p.date, p.group, p.value
            from mindsdb.{self.VIEW_NAME} as t join mindsdb.{self.MODEL_NAME} as p
            where t.date = '{since}'
        """
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)
        data = to_dicts(resp["data"])
        assert len(data) == 6  # 2 groups * 3 horizon
        assert len([x for x in data if x["group"] == "a"]) == 3
        assert data[0]["date"] == (datetime.date.today() + datetime.timedelta(days=16))
