import time
import tempfile
import json

import requests
import pytest
import mysql.connector

from tests.utils.config import MYSQL_API_ROOT, HTTP_API_ROOT

# pymysql.connections.DEBUG = True


class Dlist(list):
    """Service class for convenient work with list of dicts(db response)"""

    def __contains__(self, item):
        if len(self) == 0:
            return False
        if item in self.__getitem__(0):
            return True
        return False

    def get_record(self, key, value):
        if key in self:
            for x in self:
                if x[key] == value:
                    return x
        return None


class BaseStuff:
    """Contais some helpful set of methods and attributes for tests execution."""
    predictor_name = 'home_rentals'
    file_datasource_name = "from_files"

    def query(self, _query, encoding='utf-8'):

        cnx = mysql.connector.connect(
            host=MYSQL_API_ROOT,
            port=47335,
            database='mindsdb',
            user='mindsdb',
        )
        # Force mysql to use either the text or binary protocol
        cursor = cnx.cursor(prepared=self.use_binary)

        for subquery in _query.split(';'):
            # multiple queries in one string
            if subquery.strip() == '':
                continue
            cursor.execute(subquery)

        if cursor.description:
            columns = [i[0] for i in cursor.description]
            data = cursor.fetchall()

            res = Dlist()
            for row in data:
                res.append(dict(zip(columns, row)))

        else:
            res = {}

        return res

    def create_database(self, name, db_data):
        db_type = db_data["type"]
        # Drop any existing DB with this name to avoid conflicts
        self.query(f'DROP DATABASE IF EXISTS {name};')
        self.query(f"CREATE DATABASE {name} WITH ENGINE = '{db_type}', PARAMETERS = {json.dumps(db_data['connection_data'])};")

    def upload_ds(self, df, name):
        """Upload pandas df as csv file."""
        self.query(f"DROP TABLE files.{name};")
        with tempfile.NamedTemporaryFile(mode='w+', newline='', delete=False) as f:
            df.to_csv(f, index=False)
            filename = f.name
            f.close()

        with open(filename, 'r') as f:
            url = f'{HTTP_API_ROOT}/files/{name}'
            data = {
                "name": (name, f, 'text/csv')
            }
            res = requests.put(url, files=data)
            res.raise_for_status()

    def verify_file_ds(self, ds_name):
        timeout = 5
        threshold = time.time() + timeout
        res = ''
        while time.time() < threshold:
            res = self.query("SHOW files.tables;")
            if 'Tables_in_files' in res and res.get_record('Tables_in_files', ds_name):
                break
            time.sleep(0.5)
        assert 'Tables_in_files' in res and res.get_record('Tables_in_files', ds_name), f"file datasource {ds_name} is not ready to use after {timeout} seconds"

    def check_predictor_readiness(self, predictor_name):
        timeout = 600
        threshold = time.time() + timeout
        res = ''
        while time.time() < threshold:
            _query = "SELECT status, error FROM mindsdb.models WHERE name='{}';".format(predictor_name)
            res = self.query(_query)
            if 'status' in res:
                if res.get_record('status', 'complete'):
                    break
                elif res.get_record('status', 'error'):
                    raise Exception(res[0]['error'])
            time.sleep(2)
        assert 'status' in res and res.get_record('status', 'complete'), f"predictor {predictor_name} is not complete after {timeout} seconds"

    def validate_database_creation(self, name):
        res = self.query(f"SELECT name FROM information_schema.databases WHERE name='{name}';")
        assert "name" in res and res.get_record("name", name), f"Expected datasource is not found after creation - {name}: {res}"


@pytest.mark.parametrize("use_binary", (False, True), indirect=True)
class TestMySqlApi(BaseStuff):

    @pytest.fixture
    def use_binary(self, request):
        self.use_binary = request.param

    def test_create_postgres_datasources(self, use_binary):
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
        self.create_database("test_demo_postgres", db_details)
        self.validate_database_creation("test_demo_postgres")

    def test_create_mariadb_datasources(self, use_binary):
        db_details = {
            "type": "mariadb",
            "connection_data": {
                "type": "mariadb",
                "host": "samples.mindsdb.com",
                "port": "3307",
                "user": "demo_user",
                "password": "demo_password",
                "database": "test_data"
            }
        }
        self.create_database("test_demo_mariadb", db_details)
        self.validate_database_creation("test_demo_mariadb")

    def test_create_mysql_datasources(self, use_binary):
        db_details = {
            "type": "mysql",
            "connection_data": {
                "type": "mysql",
                "host": "samples.mindsdb.com",
                "port": "3306",
                "user": "user",
                "password": "MindsDBUser123!",
                "database": "public"
            }
        }
        self.create_database("test_demo_mysql", db_details)
        self.validate_database_creation("test_demo_mysql")

    # TODO fix these after float/bool type issue is fixed

    # def test_create_predictor(self, use_binary):
    #     self.query(f"DROP MODEL IF EXISTS {self.predictor_name};")
    #     add file lock here
    #     self.query(f"CREATE MODEL {self.predictor_name} from test_demo_mysql (select * from test_demo_mysql.home_rentals) PREDICT rental_price;")
    #     self.check_predictor_readiness(self.predictor_name)

    # def test_making_prediction(self, use_binary):
    #     _query = f"""
    #         SELECT rental_price, rental_price_explain
    #         FROM {self.predictor_name}
    #         WHERE number_of_rooms = 2 and sqft = 400 and location = 'downtown' and days_on_market = 2 and initial_price= 2500;
    #     """
    #     res = self.query(_query)
    #     assert 'rental_price' in res and 'rental_price_explain' in res, f"error getting prediction from {self.predictor_name} - {res}"

    # @pytest.mark.parametrize("describe_attr", ["model", "features", "ensemble"])
    # def test_describe_predictor_attrs(self, describe_attr):
    #     self.query(f"describe mindsdb.{self.predictor_name}.{describe_attr};")

    @pytest.mark.parametrize("query", [
        "show databases;",
        "show schemas;",
        "show tables;",
        "show tables from mindsdb;",
        "show tables in mindsdb;",
        "show full tables from mindsdb;",
        "show full tables in mindsdb;",
        "show variables;",
        "show session status;",
        "show global variables;",
        "show engines;",
        "show warnings;",
        "show charset;",
        "show collation;",
        # TODO fix these after float/bool type issue is fixed
        # "show models;",
        # "show function status where db = 'mindsdb';",
        # "show procedure status where db = 'mindsdb';"
    ])
    def test_service_requests(self, query, use_binary):
        self.query(query)

    # def test_show_columns(self, use_binary):
    #     ret = self.query("""
    #         SELECT
    #             *
    #         FROM information_schema.columns
    #         WHERE table_name = 'rentals' and table_schema='postgres'
    #     """)
    #     assert len(ret) == 8
    #     assert sorted([x['ORDINAL_POSITION'] for x in ret]) == list(range(1, 9))

    #     rental_price_column = next(x for x in ret if x['COLUMN_NAME'] == 'rental_price')
    #     assert rental_price_column['DATA_TYPE'] == 'double'
    #     assert rental_price_column['COLUMN_TYPE'] == 'double'
    #     assert rental_price_column['ORIGINAL_TYPE'] == 'double precision'
    #     assert rental_price_column['NUMERIC_PRECISION'] is not None

    #     location_column = next(x for x in ret if x['COLUMN_NAME'] == 'location')
    #     assert location_column['DATA_TYPE'] == 'varchar'
    #     assert location_column['COLUMN_TYPE'].startswith('varchar(')    # varchar(###)
    #     assert location_column['ORIGINAL_TYPE'] == 'character varying'
    #     assert location_column['NUMERIC_PRECISION'] is None
    #     assert location_column['CHARACTER_MAXIMUM_LENGTH'] is not None
    #     assert location_column['CHARACTER_OCTET_LENGTH'] is not None

    # TODO fix these after float/bool type issue is fixed
    # test
    # def test_train_model_from_files(self):
    #     df = pd.DataFrame({
    #         'x1': [x for x in range(100, 210)] + [x for x in range(100, 210)],
    #         'x2': [x * 2 for x in range(100, 210)] + [x * 3 for x in range(100, 210)],
    #         'y': [x * 3 for x in range(100, 210)] + [x * 2 for x in range(100, 210)]
    #     })
    #     file_predictor_name = "predictor_from_file"
    #     self.upload_ds(df, self.file_datasource_name)
    #     self.verify_file_ds(self.file_datasource_name)

    #     self.query(f"DROP MODEL IF EXISTS mindsdb.{file_predictor_name};")
    #     add file lock here
    #     _query = f"""
    #         CREATE MODEL mindsdb.{file_predictor_name}
    #         from files (select * from {self.file_datasource_name})
    #         predict y;
    #     """
    #     self.query(_query)
    #     self.check_predictor_readiness(file_predictor_name)

    # TODO fix these after float/bool type issue is fixed
    # def test_select_from_files(self, use_binary):
    #     _query = f"select * from files.{self.file_datasource_name};"
    #     self.query(_query)

    # TODO fix these after float/bool type issue is fixed
    # def test_ts_train_and_predict(self, subtests, use_binary):
    #     train_df = pd.DataFrame({
    #         'gby': ["A" for _ in range(100, 210)] + ["B" for _ in range(100, 210)],
    #         'oby': [x for x in range(100, 210)] + [x for x in range(200, 310)],
    #         'x1': [x for x in range(100, 210)] + [x for x in range(100, 210)],
    #         'x2': [x * 2 for x in range(100, 210)] + [x * 3 for x in range(100, 210)],
    #         'y': [x * 3 for x in range(100, 210)] + [x * 2 for x in range(100, 210)]
    #     })

    #     test_df = pd.DataFrame({
    #         'gby': ["A" for _ in range(210, 220)] + ["B" for _ in range(210, 220)],
    #         'oby': [x for x in range(210, 220)] + [x for x in range(310, 320)],
    #         'x1': [x for x in range(210, 220)] + [x for x in range(210, 220)],
    #         'x2': [x * 2 for x in range(210, 220)] + [x * 3 for x in range(210, 220)],
    #         'y': [x * 3 for x in range(210, 220)] + [x * 2 for x in range(210, 220)]
    #     })

    #     train_ds_name = "train_ts_file_ds"
    #     test_ds_name = "test_ts_file_ds"
    #     for df, ds_name in [(train_df, train_ds_name), (test_df, test_ds_name)]:
    #         self.upload_ds(df, ds_name)
    #         self.verify_file_ds(ds_name)

    #     params = [
    #         ("with_group_by_hor1",
    #             f"CREATE MODEL mindsdb.%s from files (select * from {train_ds_name}) PREDICT y ORDER BY oby GROUP BY gby WINDOW 10 HORIZON 1;",
    #             f"SELECT res.gby, res.y as PREDICTED_RESULT FROM files.{test_ds_name} as source JOIN mindsdb.%s as res WHERE source.gby= 'A' LIMIT 1;",
    #             1),
    #         ("no_group_by_hor1",
    #             f"CREATE MODEL mindsdb.%s from files (select * from {train_ds_name}) PREDICT y ORDER BY oby WINDOW 10 HORIZON 1;",
    #             f"SELECT res.gby, res.y as PREDICTED_RESULT FROM files.{test_ds_name} as source JOIN mindsdb.%s as res LIMIT 1;",
    #             1),
    #         ("with_group_by_hor2",
    #             f"CREATE MODEL mindsdb.%s from files (select * from {train_ds_name}) PREDICT y ORDER BY oby GROUP BY gby WINDOW 10 HORIZON 2;",
    #             f"SELECT res.gby, res.y as PREDICTED_RESULT FROM files.{test_ds_name} as source JOIN mindsdb.%s as res WHERE source.gby= 'A' LIMIT 2;",
    #             2),
    #         ("no_group_by_hor2",
    #             f"CREATE MODEL mindsdb.%s from files (select * from {train_ds_name}) PREDICT y ORDER BY oby WINDOW 10 HORIZON 2;",
    #             f"SELECT res.gby, res.y as PREDICTED_RESULT FROM files.{test_ds_name} as source JOIN mindsdb.%s as res LIMIT 2;",
    #             2),
    #     ]
    #     for predictor_name, create_query, select_query, res_len in params:
    #         add file lock here
    #         with subtests.test(msg=predictor_name,
    #                            predictor_name=predictor_name,
    #                            create_query=create_query,
    #                            select_query=select_query,
    #                            res_len=res_len):
    #             self.query(create_query % predictor_name)
    #             self.check_predictor_readiness(predictor_name)
    #             res = self.query(select_query % predictor_name)
    #             assert len(res) == res_len, f"prediction result {res} contains more that {res_len} records"

    # TODO fix these after float/bool type issue is fixed
    # def test_tableau_queries(self, subtests, use_binary):
    #     test_ds_name = self.file_datasource_name
    #     predictor_name = "predictor_from_file"
    #     integration = "files"

    #     queries = [
    #         f'''
    #            SELECT TABLE_NAME,TABLE_COMMENT,IF(TABLE_TYPE='BASE TABLE', 'TABLE', TABLE_TYPE),
    #            TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES
    #            WHERE TABLE_SCHEMA LIKE '{integration}'
    #             AND ( TABLE_TYPE='BASE TABLE' OR TABLE_TYPE='VIEW' ) ORDER BY TABLE_SCHEMA, TABLE_NAME
    #         ''',
    #         f'''
    #             SELECT SUM(1) AS `cnt__0B4A4E8BD11C48FFB4730D4D2C32191A_ok`,
    #               max(`Custom SQL Query`.`x1`) AS `sum_height_ok`,
    #               max(`Custom SQL Query`.`y`) AS `sum_length1_ok`
    #             FROM (
    #               SELECT res.x1, res.y
    #                FROM files.{test_ds_name} as source
    #                JOIN mindsdb.{predictor_name} as res
    #             ) `Custom SQL Query`
    #             HAVING (COUNT(1) > 0)
    #         ''',
    #         f'''
    #             SHOW FULL TABLES FROM {integration}
    #         ''',
    #         '''
    #             SELECT `table_name`, `column_name`
    #             FROM `information_schema`.`columns`
    #             WHERE `data_type`='enum' AND `table_schema`='views';
    #         ''',
    #         '''
    #             SHOW KEYS FROM `mindsdb`.`predictors`
    #         ''',
    #         '''
    #             show full columns from `predictors`
    #         ''',
    #         '''
    #             SELECT `table_name`, `column_name` FROM `information_schema`.`columns`
    #              WHERE `data_type`='enum' AND `table_schema`='mindsdb'
    #         ''',
    #         f'''
    #             SELECT `Custom SQL Query`.`x1` AS `height`,
    #               `Custom SQL Query`.`y` AS `length1`
    #             FROM (
    #                SELECT res.x1, res.y
    #                FROM files.{test_ds_name} as source
    #                JOIN mindsdb.{predictor_name} as res
    #             ) `Custom SQL Query`
    #             LIMIT 100
    #         ''',
    #         f'''
    #         SELECT
    #           `Custom SQL Query`.`x1` AS `x1`,
    #           SUM(`Custom SQL Query`.`y2`) AS `sum_y2_ok`
    #         FROM (
    #            SELECT res.x1, res.y as y2
    #            FROM files.{test_ds_name} as source
    #            JOIN mindsdb.{predictor_name} as res
    #         ) `Custom SQL Query`
    #         GROUP BY 1
    #         ''',
    #         f'''
    #         SELECT
    #           `Custom SQL Query`.`x1` AS `x1`,
    #           COUNT(DISTINCT TRUNCATE(`Custom SQL Query`.`y`,0)) AS `ctd_y_ok`
    #         FROM (
    #            SELECT res.x1, res.y
    #            FROM files.{test_ds_name} as source
    #            JOIN mindsdb.{predictor_name} as res
    #         ) `Custom SQL Query`
    #         GROUP BY 1
    #         ''',
    #     ]
    #     for i, _query in enumerate(queries):
    #         with subtests.test(msg=i, _query=_query):
    #             self.query(_query)
