import tempfile
import unittest
import json
from pathlib import Path

import docker
import netifaces

from common import HTTP_API_ROOT, run_environment, EXTERNAL_DB_CREDENTIALS, USE_EXTERNAL_DB_SERVER, CONFIG_PATH

def get_docker0_inet_ip():
    if "docker0" not in netifaces.interfaces():
        raise Exception("Unable to find 'docker' interface. Please install docker first.")
    return netifaces.ifaddresses('docker0')[netifaces.AF_INET][0]['addr']


class MySqlApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        override_config = {
                'integrations': {},
                'api': {
                    "http": {"host": get_docker0_inet_ip()},
                    "mysql": {"host": get_docker0_inet_ip()}
                    }
                }

        run_environment(apis=['http', 'mysql'], override_config=override_config)
        cls.docker_client = docker.from_env()
        cls.mysql_image = 'mysql'

        cls.config = json.loads(Path(CONFIG_PATH).read_text())

        with open(EXTERNAL_DB_CREDENTIALS, 'rt') as f:
            cls.db_creds = json.load(f)

        cls.launch_query_tmpl = "mysql --host=%s --port=%s --user=%s --database=mindsdb" % (cls.config["api"]["mysql"]["host"],
                                                                       cls.config["api"]["mysql"]["port"],
                                                                       cls.config["api"]["mysql"]["user"])

    @classmethod
    def tearDownClass(cls):
        cls.docker_client.close()


    def query(self, _query):
        print(f'API MYSQL password: {self.config["api"]["mysql"]["password"]}')
        with tempfile.TemporaryDirectory() as tmpdirname:
            with open(f"{tmpdirname}/test.sql", 'w') as f:
                f.write(_query)
            cmd = f"{self.launch_query_tmpl} < /temp/test.sql"
            cmd = 'sh -c "' + cmd + '"'
            res = self.docker_client.containers.run(self.mysql_image,
                                                 command=cmd,
                                                 remove=True,
                                                 volumes={str(tmpdirname): {'bind': '/temp', 'mode': 'ro'}},
                                                 environment={"MYSQL_PWD": self.config["api"]["mysql"]["password"]})
        return res

    def create_datasource(self, db_type):
        _query = "CREATE DATASOURCE %s WITH ENGINE='%s',PARAMETERS='%s';" % (db_type.upper(), db_type, json.dumps(self.db_creds[db_type]))
        return self.query(_query)

    def validate_datasource_creation(self, ds_type):
        self.create_datasource(ds_type.lower())
        res = self.query("SELECT * FROM datasources WHERE name='{}';".format(ds_type.upper())).decode('utf-8')
        print("Select result: ", res)
        self.assertTrue(ds_type.upper() in res,
                        f"Expected datasource is not found after creation - {ds_type.upper()}")

    def test_create_mysql_datasource_query(self):
        print(f"\nExecuting {self._testMethodName}")
        self.validate_datasource_creation('mysql')
        
if __name__ == "__main__":
    try:
        unittest.main(failfast=True)
        print('Tests passed!')
    except Exception as e:
        print(f'Tests Failed!\n{e}')
