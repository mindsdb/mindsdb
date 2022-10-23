import time
import os
import shutil
import tarfile

import pytest
import docker

from mindsdb.integrations.handlers.mysql_handler.mysql_handler import MySQLHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE

HANDLER_KWARGS = {"connection_data": {
                    "host": "localhost",
                    "port": "13306",
                    "user": "root",
                    "password": "supersecret",
                    "database": "test",
                    "ssl": False
             }
}

CERTS_ARCHIVE = "certs.tar"
CERTS_DIR = "mysql"

def get_certs():
    certs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mysql")
    certs = {}
    for cert_key, fname in [("ssl_ca", "ca.pem"), ("ssl_cert", "client-cert.pem"), ("ssl_key", "client-key.pem")]:
        cert_file = os.path.join(certs_dir, fname)
        certs[cert_key] = cert_file
    return certs

def get_certificates(container):
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    archive_path = os.path.join(cur_dir, CERTS_ARCHIVE)
    with open(archive_path, "wb") as f:
        bits, _ = container.get_archive('/var/lib/mysql')
        for chunk in bits:
            f.write(chunk)

    with tarfile.open(archive_path) as tf:
        tf.extractall(path=cur_dir)
    certs = get_certs()
    HANDLER_KWARGS["connection_data"].update(certs)


def waitReadiness(container, timeout=30):
    threshold = time.time() + timeout
    ready_msg = "mariadbd: ready for connections"
    while True:
        lines = container.logs().decode()
        # container fully ready
        # because it reloads the db server during initialization
        # need to check that the 'ready for connections' has found second time
        if lines.count(ready_msg) >= 2:
            break
        if time.time() > threshold:
            raise Exception("timeout exceeded, container is still not ready")

@pytest.fixture(scope="module", params=[{"ssl": False}, {"ssl": True}], ids=["NoSSL", "SSL"])
def handler(request):
    image_name = "mindsdb/mariadb-handler-test"
    docker_client = docker.from_env()
    with_ssl = request.param["ssl"]
    container = None
    try:
        container = docker_client.containers.run(
                    image_name,
                    command="--secure-file-priv=/",
                    detach=True,
                    environment={"MYSQL_ROOT_PASSWORD":"supersecret"},
                    ports={"3306/tcp": 13306},
                )
        waitReadiness(container)
    # ubnormal teardown
    except Exception as e:
        if container is not None:
            container.kill()
        raise e

    if with_ssl:
        get_certificates(container)
    handler = MySQLHandler('test_mysql_handler', **HANDLER_KWARGS)
    yield handler

    # normal teardown
    container.kill()
    docker_client.close()
    if with_ssl:
        cur_dir = os.path.dirname(os.path.abspath(__file__))
        try:
            os.remove(os.path.join(cur_dir, CERTS_ARCHIVE))
            shutil.rmtree(os.path.join(cur_dir, CERTS_DIR))
        except Exception as e:
            print(f"unable to delete .tar/files of certificates: {e}")


class TestMariaDBHandler:
    def test_connect(self, handler):
        handler.connect()
        assert handler.is_connected, "connection error"

    def test_check_connection(self, handler):
        res = handler.check_connection()
        assert res.success, res.error_message

    def test_native_query_show_dbs(self, handler):
        dbs = handler.native_query("SHOW DATABASES;")
        dbs = dbs.data_frame
        assert dbs is not None, "expected to get some data, but got None"
        assert 'Database' in dbs, f"expected to get 'Database' column in response:\n{dbs}"
        dbs = list(dbs["Database"])
        expected_db = HANDLER_KWARGS["connection_data"]["database"]
        assert expected_db in dbs, f"expected to have {expected_db} db in response: {dbs}"

    def test_get_tables(self, handler):
        tables = self.get_table_names(handler)
        assert "rentals" in tables, f"expected to have 'rentals' table in the db but got: {tables}"

    def test_describe_table(self, handler):
        described = handler.get_columns("rentals")
        describe_data = described.data_frame
        self.check_valid_response(described)
        got_columns = list(describe_data.iloc[:, 0])
        want_columns = ["number_of_rooms", "number_of_bathrooms",
                        "sqft", "location", "days_on_market",
                        "initial_price", "neighborhood", "rental_price"]
        assert got_columns == want_columns, f"expected to have next columns in rentals table:\n{want_columns}\nbut got:\n{got_columns}"

    def test_create_table(self, handler):
        new_table = "test_mdb"
        res = handler.native_query(f"CREATE TABLE IF NOT EXISTS {new_table} (test_col INT)")
        self.check_valid_response(res)
        tables = self.get_table_names(handler)
        assert new_table in tables, f"expected to have {new_table} in database, but got: {tables}"

    def test_drop_table(self, handler):
        drop_table = "test_md"
        res = handler.native_query(f"DROP TABLE IF EXISTS {drop_table}")
        self.check_valid_response(res)
        tables = self.get_table_names(handler)
        assert drop_table not in tables

    def test_select_query(self, handler):
        limit = 5
        query = f"SELECT * FROM rentals WHERE number_of_rooms = 2 LIMIT {limit}"
        res = handler.query(query)
        self.check_valid_response(res)
        got_rows = res.data_frame.shape[0]
        want_rows = limit
        assert got_rows == want_rows, f"expected to have {want_rows} rows in response but got: {got_rows}"

    def check_valid_response(self, res):
        if res.resp_type == RESPONSE_TYPE.TABLE:
            assert res.data_frame is not None, "expected to have some data, but got None"
        assert res.error_code == 0, f"expected to have zero error_code, but got {df.error_code}"
        assert res.error_message is None, f"expected to have None in error message, but got {df.error_message}"

    def get_table_names(self, handler):
        res = handler.get_tables()
        tables = res.data_frame
        assert tables is not None, "expected to have some tables in the db, but got None"
        assert 'table_name' in tables, f"expected to get 'table_name' column in the response:\n{tables}"
        return list(tables['table_name'])
