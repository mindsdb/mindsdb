import time
import os
import shutil
import tarfile
import unittest
import docker

from mindsdb.integrations.handlers.mysql_handler.mysql_handler import MySQLHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


def get_certs():
    certs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mysql")
    certs = {}
    for cert_key, fname in [("ssl_ca", "ca.pem"), ("ssl_cert", "client-cert.pem"), ("ssl_key", "client-key.pem")]:
        cert_file = os.path.join(certs_dir, fname)
        certs[cert_key] = cert_file
    return certs


class MySQLNoSSLHandlerTest(unittest.TestCase):
    image_name = "mindsdb/mysql-handler-test"
    kwargs = {"connection_data": {
                        "host": "localhost",
                        "port": "3307",
                        "user": "root",
                        "password": "supersecret",
                        "database": "test",
                        "ssl": False
                 }
    }
    certs_archive = "certs.tar"
    certs_dir = "mysql"
    @classmethod
    def setUpClass(cls):
        cls.containers = list()
        cls.docker_client = docker.from_env()
        main_container = cls.docker_client.containers.run(
                    cls.image_name,
                    command="--secure-file-priv=/",
                    detach=True,
                    environment={"MYSQL_ROOT_PASSWORD":"supersecret"},
                    ports={"3306/tcp": 3307},
                )
        cls.containers.append(main_container)
        cls.waitReadiness(main_container)
        cls.get_certificates()
        cls.handler = MySQLHandler('test_mysql_handler', **cls.kwargs)

    @classmethod
    def get_certificates(cls):
        pass

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
        cur_dir = os.path.dirname(os.path.abspath(__file__))
        try:
            os.remove(os.path.join(cur_dir, cls.certs_archive))
            shutil.rmtree(os.path.join(cur_dir, cls.certs_dir))
        except Exception as e:
            print(f"unable to delete .tar/files of certificates: {e}")

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

    def test_2_get_tables(self):
        tables = self.get_table_names()
        assert "rentals" in tables, f"expected to have 'rentals' table in the db but got: {tables}"

    def test_3_describe_table(self):
        described = self.handler.get_columns("rentals")
        describe_data = described.data_frame
        self.check_valid_response(described)
        got_columns = list(describe_data.iloc[:, 0])
        want_columns = ["number_of_rooms", "number_of_bathrooms",
                        "sqft", "location", "days_on_market",
                        "initial_price", "neighborhood", "rental_price"]
        assert got_columns == want_columns, f"expected to have next columns in rentals table:\n{want_columns}\nbut got:\n{got_columns}"

    def test_4_create_table(self):
        new_table = "test_mdb"
        res = self.handler.native_query(f"CREATE TABLE IF NOT EXISTS {new_table} (test_col INT)")
        self.check_valid_response(res)
        tables = self.get_table_names()
        assert new_table in tables, f"expected to have {new_table} in database, but got: {tables}"

    def test_5_drop_table(self):
        drop_table = "test_md"
        res = self.handler.native_query(f"DROP TABLE IF EXISTS {drop_table}")
        self.check_valid_response(res)
        tables = self.get_table_names()
        assert drop_table not in tables

    def test_6_select_query(self):
        limit = 5
        query = f"SELECT * FROM rentals WHERE number_of_rooms = 2 LIMIT {limit}"
        res = self.handler.query(query)
        self.check_valid_response(res)
        got_rows = res.data_frame.shape[0]
        want_rows = limit
        assert got_rows == want_rows, f"expected to have {want_rows} rows in response but got: {got_rows}"

    def check_valid_response(self, res):
        if res.resp_type == RESPONSE_TYPE.TABLE:
            assert res.data_frame is not None, "expected to have some data, but got None"
        assert res.error_code == 0, f"expected to have zero error_code, but got {df.error_code}"
        assert res.error_message is None, f"expected to have None in error message, but got {df.error_message}"

    def get_table_names(self):
        res = self.handler.get_tables()
        tables = res.data_frame
        assert tables is not None, "expected to have some tables in the db, but got None"
        assert 'table_name' in tables, f"expected to get 'table_name' column in the response:\n{tables}"
        return list(tables['table_name'])


class MySQLSSLHandlerTest(MySQLNoSSLHandlerTest):
    kwargs = {"connection_data": {
                        "host": "localhost",
                        "port": "3307",
                        "user": "ssl_user",
                        "password": "ssl",
                        "database": "test",
                        "ssl": True
                 }
    }

    @classmethod
    def get_certificates(cls):
        c = cls.containers[0]
        cur_dir = os.path.dirname(os.path.abspath(__file__))
        archive_path = os.path.join(cur_dir, cls.certs_archive)
        with open(archive_path, "wb") as f:
            bits, _ = c.get_archive('/var/lib/mysql')
            for chunk in bits:
                f.write(chunk)

        with tarfile.open(archive_path) as tf:
            tf.extractall(path=cur_dir)
        certs = get_certs()
        cls.kwargs["connection_data"].update(certs)


if __name__ == "__main__":
    unittest.main(failfast=True, verbosity=2)
