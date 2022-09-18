import time
import unittest
import docker

from mindsdb.integrations.handlers.mysql_handler.mysql_handler import MySQLHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


class MySQLHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {"connection_data": {
                            "host": "localhost",
                            "port": "3307",
                            "user": "root",
                            "password": "supersecret",
                            "database": "test",
                            "ssl": False
                     }
        }
        cls.containers = list()
        cls.docker_client = docker.from_env()
        main_container = cls.docker_client.containers.run(
                    "my-mysql",
                    command="--secure-file-priv=/",
                    detach=True,
                    environment={"MYSQL_ROOT_PASSWORD":"supersecret"},
                    ports={"3306/tcp": 3307},
                )
        cls.containers.append(main_container)
        cls.waitReadiness(main_container)
        cls.handler = MySQLHandler('test_mysql_handler', **cls.kwargs)

    @classmethod
    def waitReadiness(cls, container, timeout=15):
        logs_inter = container.logs(stream=True)
        threshold = time.time() + timeout
        ready_event_num = 0
        while True:
            line = logs_inter.next().decode()
            if "/usr/sbin/mysqld: ready for connections. Version: '8.0.27'" in line:
                ready_event_num += 1
                # container fully ready
                # because it reloads the db server during initialization
                # need to check that the 'ready for connections' has found second time
                if ready_event_num > 1:
                    break
            if time.time() > threshold:
                raise Exception("timeout exceeded, container still not ready")



    @classmethod
    def tearDownClass(cls):
        for c in cls.containers:
            c.kill()
        cls.docker_client.close()

    def test_00_connect(self):
        self.handler.connect()
        assert self.handler.is_connected, "connection error"

    def test_01_check_connection(self):
        res = self.handler.check_connection()
        assert res.success, res.error_message

    def test_1_native_query_show_dbs(self):
        dbs = self.handler.native_query("SHOW DATABASES;")
        dbs = dbs.data_frame
        assert dbs is not None, "expected to get some data, but got None"
        assert 'Database' in dbs, f"expected to get 'Database' column in response:\n{dbs}"
        dbs = list(dbs["Database"])
        expected_db = self.kwargs["connection_data"]["database"]
        assert expected_db in dbs, f"expected to have {expected_db} db in response: {dbs}"

    # def test_2_get_tables(self):
    #     tbls = self.handler.get_tables()
    #     assert tbls['type'] is not RESPONSE_TYPE.ERROR

    # def test_3_get_views(self):
    #     views = self.handler.get_views()
    #     assert views['type'] is not RESPONSE_TYPE.ERROR

    # def test_5_drop_table(self):
    #     res = self.handler.native_query("DROP TABLE IF EXISTS test_mdb")
    #     assert res['type'] is not RESPONSE_TYPE.ERROR 

    # def test_4_create_table(self):
    #     res = self.handler.native_query("CREATE TABLE IF NOT EXISTS test_mdb (test_col INT)")
    #     assert res['type'] is not RESPONSE_TYPE.ERROR 

    # def test_6_describe_table(self):
    #     described = self.handler.get_columns("test_mdb")
    #     assert described['type'] is RESPONSE_TYPE.TABLE

    # def test_7_select_query(self):
    #     query = "SELECT * FROM test_mdb WHERE 'id'='a'"
    #     result = self.handler.query(query)
    #     assert result['type'] is RESPONSE_TYPE.TABLE


if __name__ == "__main__":
    unittest.main(failfast=True, verbosity=2)
