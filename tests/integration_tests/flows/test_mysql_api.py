import time
import tempfile
import unittest
import json
from pathlib import Path

import docker
import netifaces

from common import (
    run_environment,
    EXTERNAL_DB_CREDENTIALS,
    CONFIG_PATH)


class Dlist(list):
    """Service class for convinient work with list of dicts(db response)"""

    def __contains__(self, item):
        if item in self.__getitem__(0):
            return True
        return False

    def get_record(self, key, value):
        if key in self:
            for x in self:
                if x[key] == value:
                    return x
        return None


def get_docker0_inet_ip():
    if "docker0" not in netifaces.interfaces():
        raise Exception("Unable to find 'docker' interface. Please install docker first.")
    return netifaces.ifaddresses('docker0')[netifaces.AF_INET][0]['addr']


class MySqlApiTest(unittest.TestCase):
    predictor_name = 'home_rentals'
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

        cls.launch_query_tmpl = "mysql --host=%s --port=%s --user=%s --database=mindsdb" % (
            cls.config["api"]["mysql"]["host"],
            cls.config["api"]["mysql"]["port"],
            cls.config["api"]["mysql"]["user"])

    @classmethod
    def tearDownClass(cls):
        cls.docker_client.close()

    def query(self, _query, encoding='utf-8'):
        """Run mysql docker container
           Perform connection to mindsdb database
           Execute sql request
           ----------------------
           It is very problematic (or even impossible)
           to provide sql statement as it is in 'docker run command',
           that's why this action is splitted on three steps:
               Save sql statement into temporary dir in .sql file
               Run docker container with volume points to this temp dir,
               Provide .sql file as input parameter for 'mysql' command"""
        with tempfile.TemporaryDirectory() as tmpdirname:
            with open(f"{tmpdirname}/test.sql", 'w') as f:
                f.write(_query)
            cmd = f"{self.launch_query_tmpl} < /temp/test.sql"
            cmd = 'sh -c "' + cmd + '"'
            res = self.docker_client.containers.run(
                self.mysql_image,
                command=cmd,
                remove=True,
                volumes={str(tmpdirname): {'bind': '/temp', 'mode': 'ro'}},
                environment={"MYSQL_PWD": self.config["api"]["mysql"]["password"]})
        return self.to_dicts(res.decode(encoding))

    @staticmethod
    def to_dicts(response):
        if not response:
            return {}
        lines = response.splitlines()
        if len(lines) < 2:
            return {}
        headers = tuple(lines[0].split("\t"))
        res = Dlist()
        for body in lines[1:]:
            data = tuple(body.split("\t"))
            res.append(dict(zip(headers, data)))
        return res

    def create_datasource(self, db_type):
        _query = "CREATE DATASOURCE %s WITH ENGINE = '%s', PARAMETERS = %s;" % (
            db_type.upper(),
            db_type,
            json.dumps(self.db_creds[db_type]))
        return self.query(_query)

    def validate_datasource_creation(self, ds_type):
        self.create_datasource(ds_type.lower())
        res = self.query("SELECT * FROM mindsdb.datasources WHERE name='{}';".format(ds_type.upper()))
        self.assertTrue("name" in res and res.get_record("name", ds_type.upper()),
                        f"Expected datasource is not found after creation - {ds_type.upper()}: {res}")

    def test_1_create_datasources(self):
        for ds_type in self.db_creds:
            if ds_type not in ['kafka', 'redis', 'mongodb_atlas']:
                with self.subTest(msg=ds_type):
                    print(f"\nExecuting {self._testMethodName} ({__name__}.{self.__class__.__name__}) [{ds_type}]")
                    self.validate_datasource_creation(ds_type)

    def test_2_create_predictor(self):
        _query = f"CREATE PREDICTOR {self.predictor_name} from MYSQL (select * from test_data.home_rentals) as hr_ds predict rental_price;"
        self.query(_query)
        timeout = 600
        threshold = time.time() + timeout
        res = ''
        while time.time() < threshold:
            _query = "SELECT status FROM predictors WHERE name='{}';".format(self.predictor_name)
            res = self.query(_query)
            if 'status' in res and res.get_record('status', 'complete'):
                break
            time.sleep(2)
        self.assertTrue('status' in res and res.get_record('status', 'complete'),
                        f"predictor {self.predictor_name} is not complete after {timeout} seconds")

    def test_3_making_prediction(self):
        _query = ('SELECT rental_price, rental_price_explain FROM ' +
                  self.predictor_name +
                  ' WHERE when_data=\'{"number_of_rooms":"2","sqft":"400","location":"downtown","days_on_market":"2","initial_price":"2500"}\';')
        res = self.query(_query)
        self.assertTrue('rental_price' in res and 'rental_price_explain' in res,
                        f"error getting prediction from {self.predictor_name} - {res}")

    def test_4_service_requests(self):
        service_requests = [
            "show databases;",
            "show schemas;",
            "show tables;",
            "show tables from mindsdb;",
            "show full tables from mindsdb;",
            "show variables;",
            "show session status;",
            "show global variables;",
            "show engines;",
            "show warnings;",
            "show charset;",
            "show collation;",
            "show datasources;",
            "show predictors;",
            "show function status where db = 'mindsdb';",
            "show procedure status where db = 'mindsdb';",
            # "show table status like commands;",
        ]
        for req in service_requests:
            with self.subTest(msg=req):
                print(f"\nExecuting {self._testMethodName} ({__name__}.{self.__class__.__name__}) [{req}]")
                self.query(req)

    def test_5_drop_datasource(self):
        self.query('drop datasource MYSQL;')

    def test_6_describe_predictor_attrs(self):
        attrs = ["model", "features", "ensemble"]
        for attr in attrs:
            with self.subTest(msg=attr):
                print(f"\nExecuting {self._testMethodName} ({__name__}.{self.__class__.__name__}) [{attr}]")
                self.query(f"describe mindsdb.{self.predictor_name}.{attr};")


if __name__ == "__main__":
    try:
        unittest.main(failfast=True, verbosity=2)
        print('Tests passed!')
    except Exception as e:
        print(f'Tests Failed!\n{e}')
