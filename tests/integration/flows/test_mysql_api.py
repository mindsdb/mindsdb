import os
import time
import json
import tempfile
import datetime
from decimal import Decimal

import pytest
import requests
import mysql.connector
import pandas as pd

from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import DATA_C_TYPE_MAP, MYSQL_DATA_TYPE

from tests.integration.conftest import MYSQL_API_ROOT, HTTP_API_ROOT

# pymysql.connections.DEBUG = True


class Dlist(list):
    """Service class for convenient work with list of dicts(db response).
    Assumes keys are already normalized to lowercase."""

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

    predictor_name = "home_rentals"
    file_datasource_name = "from_files"

    def query(self, _query, encoding="utf-8", with_description=False):
        description = None
        with mysql.connector.connect(
            host=MYSQL_API_ROOT,
            port=47335,
            database="mindsdb",
            user="mindsdb",
        ) as cnx:
            # Force mysql to use either the text or binary protocol
            with cnx.cursor(prepared=self.use_binary) as cursor:
                for subquery in _query.split(";"):
                    # multiple queries in one string
                    if subquery.strip() == "":
                        continue
                    cursor.execute(subquery)

                if cursor.description:
                    description = cursor.description
                    columns = [i[0].lower() for i in cursor.description]
                    data = cursor.fetchall()

                    res = Dlist()
                    for row in data:
                        res.append(dict(zip(columns, row)))

                else:
                    res = {}

        if with_description:
            return res, description
        return res

    def create_database(self, name, db_data):
        db_type = db_data["type"]
        # Drop any existing DB with this name to avoid conflicts
        self.query(f"DROP DATABASE IF EXISTS {name};")
        self.query(
            f"CREATE DATABASE {name} WITH ENGINE = '{db_type}', PARAMETERS = {json.dumps(db_data['connection_data'])};"
        )

    def upload_ds(self, df, name):
        """Upload pandas df as csv file."""
        self.query(f"DROP TABLE IF EXISTS files.{name};")
        with tempfile.NamedTemporaryFile(mode="w+", newline="", delete=False) as f:
            df.to_csv(f, index=False)
            filename = f.name
            f.close()

        with open(filename, "r") as f:
            url = f"{HTTP_API_ROOT}/files/{name}"
            files = {"file": (f"{name}.csv", f, "text/csv")}
            res = requests.put(url, files=files)
            res.raise_for_status()

    def verify_file_ds(self, ds_name):
        timeout = 10
        threshold = time.time() + timeout
        res = ""
        while time.time() < threshold:
            res = self.query("SHOW tables from files;")
            if "tables_in_files" in res and res.get_record("tables_in_files", ds_name):
                return
            time.sleep(0.3)
        assert "tables_in_files" in res and res.get_record("tables_in_files", ds_name), (
            f"file datasource {ds_name} is not ready to use after {timeout} seconds"
        )

    def check_predictor_readiness(self, predictor_name):
        timeout = 600
        threshold = time.time() + timeout
        res = ""
        model_not_found_threshold = time.time() + 30
        check_interval = 1
        while time.time() < threshold:
            _query = "SELECT status, error FROM mindsdb.models WHERE name='{}';".format(predictor_name)
            res = self.query(_query)
            if "status" in res:
                if res.get_record("status", "complete"):
                    break
                elif res.get_record("status", "error"):
                    raise Exception(res[0]["error"])
            elif len(res) == 0 and time.time() > model_not_found_threshold:
                raise Exception(f"Model {predictor_name} not found in models table after 30 seconds")
            time.sleep(check_interval)
        assert "status" in res and res.get_record("status", "complete"), (
            f"predictor {predictor_name} is not complete after {timeout} seconds. Last result: {res}"
        )

    def validate_database_creation(self, name):
        res = self.query(f"SELECT name FROM information_schema.databases WHERE name='{name}';")
        assert "name" in res and res.get_record("name", name), (
            f"Expected datasource is not found after creation - {name}: {res}"
        )


@pytest.mark.parametrize("use_binary", [False, True], indirect=True)
class TestMySqlApi(BaseStuff):
    @pytest.fixture
    def use_binary(self, request):
        self.use_binary = request.param

    def test_create_postgres_datasources(self, use_binary):
        db_details = {
            "type": "postgres",
            "connection_data": {
                "host": "samples.mindsdb.com",
                "port": "5432",
                "user": "demo_user",
                "password": "demo_password",
                "database": "demo",
                "schema": "demo",
            },
        }
        self.create_database("test_demo_postgres", db_details)
        self.validate_database_creation("test_demo_postgres")

    @pytest.mark.parametrize("table_name", ["types_test_data", "types_test_data_with_nulls"])
    def test_response_types(self, use_binary, table_name):
        """Test that data conversion is correct. Postgres used as source database.
        Used two tables: with and without nulls in rows. This is because pd.DataFrame cast data
        with and without nulls in rows differently.

        Note:
            - for 'data with nulls' big 'bigint' values returned wrong. This is because
            pd.DataFrame cast 'bigint' values with nulls to 'float', therefore precision is lost.
            - we cast datatypes in binary protocol only as signed, therefore precision for unsigned
            may be lost
            - sometimes dt_date returned as datetime instead of date. Most likely reason is mysql.connector.

        Test tables created using:

        create table types_test_data (
            t_char CHAR(10),
            t_varchar VARCHAR(100),
            t_text TEXT,
            t_bytea BYTEA,
            t_json JSON,
            t_jsonb JSONB,
            t_xml XML,
            t_uuid UUID,
            n_smallint SMALLINT,
            n_integer INTEGER,
            n_bigint BIGINT,
            n_decimal DECIMAL(10,2),
            n_numeric NUMERIC(10,4),
            n_real REAL,
            n_double_precision DOUBLE PRECISION,
            n_smallserial SMALLSERIAL,
            n_serial SERIAL,
            n_bigserial BIGSERIAL,
            n_money MONEY,
            n_int2 INT2,        -- alt for SMALLINT
            n_int4 INT4,        -- alt for INTEGER
            n_int8 INT8,        -- alt for BIGINT
            n_float4 FLOAT4,    -- alt for REAL
            n_float8 FLOAT8,    -- alt for DOUBLE precision
            dt_date DATE,
            dt_time TIME,
            dt_time_tz TIME WITH TIME ZONE,
            dt_timestamp TIMESTAMP,
            dt_timestamp_tz TIMESTAMP WITH TIME ZONE,
            dt_interval INTERVAL,
            dt_timestamptz TIMESTAMPTZ,
            dt_timetz TIMETZ
        );

        insert into types_test_data values (
            -- text
            'Test',
            'Test',
            'Test',
            E'\\x44656D6F2062696E61727920646174612E',
            '{"name": "test"}',
            '{"name": "test"}',
            '<root><element>test</element><nested><value>123</value></nested></root>',
            'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
            -- numeric
            32767,                  -- n_smallint (max value)
            2147483647,             -- n_integer (max value)
            9223372036854775807,    -- n_bigint (max value)
            1234.56,                -- n_decimal
            12345.6789,             -- n_numeric
            3.14159,                -- n_real
            2.7182818284590452,     -- n_double_precision
            1, 1, 1, -- serials
            10500.25,           -- n_money ?
            255,                 -- n_int2 (min value)
            42,                     -- n_int4
            123456789,              -- n_int8
            0.00123,                -- n_float4
            9.8765432109876,        -- n_float8
            -- datetime
            '2023-10-15',                                -- t_date
            '14:30:45',                                  -- t_time
            '14:30:45+03:00',                            -- t_time_tz
            '2023-10-15 14:30:45',                       -- t_timestamp
            '2023-10-15 14:30:45+03:00',                 -- t_timestamp_tz
            '2 years 3 months 15 days 12 hours 30 minutes 15 seconds', -- t_interval
            '2023-10-15 14:30:45+03:00',                 -- t_timestamptz
            '14:30:45+03:00'                             -- t_timetz
        );

        create table types_test_data_with_nulls (...);
        insert into types_test_data_with_nulls values (same as above);
        insert into types_test_data_with_nulls DEFAULT VALUES; -- insert nulls for each column, except serials
        """
        # Test for response types
        res, description = self.query(
            f"""
            SELECT
                -- text types
                t_char, t_varchar, t_text, t_bytea, t_json, t_jsonb, t_xml, t_uuid,
                -- numeric types
                n_smallint, n_integer, n_bigint, n_decimal, n_numeric, n_real,
                n_double_precision, n_smallserial, n_serial, n_bigserial, n_money,
                -- datetime types
                dt_date,
                dt_time,
                dt_time_tz,
                dt_timestamp,
                dt_timestamp_tz,
                dt_interval,
                dt_timestamptz,
                dt_timetz
            FROM test_demo_postgres.{table_name} order by n_integer NULLS last;
        """,
            with_description=True,
        )
        expected_types = {
            # text types
            "t_char": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.TEXT],
            "t_varchar": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.VARCHAR],
            "t_text": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.TEXT],
            "t_bytea": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.BINARY],
            # text types: fallbacks to varchar
            "t_xml": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.VARCHAR],
            "t_uuid": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.VARCHAR],
            # json types
            "t_json": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.JSON],
            "t_jsonb": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.JSON],
            # numeric types
            "n_smallint": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.SMALLINT],
            "n_integer": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.INT],
            "n_bigint": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.BIGINT],
            "n_decimal": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.DECIMAL],
            "n_numeric": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.DECIMAL],
            "n_real": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.FLOAT],
            "n_double_precision": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.DOUBLE],
            "n_smallserial": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.SMALLINT],
            "n_serial": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.INT],
            "n_bigserial": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.BIGINT],
            "n_money": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.TEXT],
            # datetime types
            "dt_date": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.DATE],
            "dt_time": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.TIME],
            "dt_time_tz": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.TIME],
            "dt_timestamp": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.DATETIME],
            "dt_timestamp_tz": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.DATETIME],
            "dt_interval": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.VARCHAR],
            "dt_timestamptz": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.DATETIME],
            "dt_timetz": DATA_C_TYPE_MAP[MYSQL_DATA_TYPE.TIME],
        }
        expected_values = {
            "t_char": "Test      ",
            "t_varchar": "Test",
            "t_text": "Test",
            "t_bytea": "Demo binary data.",
            "t_json": '{"name":"test"}',
            "t_jsonb": '{"name":"test"}',
            "t_xml": "<root><element>test</element><nested><value>123</value></nested></root>",
            "t_uuid": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
            # numeric types
            "n_smallint": 32767,
            "n_integer": 2147483647,
            "n_bigint": 9223372036854775807,
            "n_decimal": Decimal("1234.56"),
            "n_numeric": Decimal("12345.6789"),
            "n_real": 3.14159,
            "n_double_precision": 2.7182818284590452,
            "n_smallserial": 1,
            "n_serial": 1,
            "n_bigserial": 1,
            "n_money": "$10,500.25",
            # datetime types
            "dt_date": datetime.date(2023, 10, 15),
            "dt_time": datetime.timedelta(seconds=52245),
            "dt_time_tz": datetime.timedelta(seconds=52245 - (3 * 60 * 60)),
            "dt_timestamp": datetime.datetime(2023, 10, 15, 14, 30, 45),
            "dt_timestamp_tz": datetime.datetime(2023, 10, 15, 11, 30, 45),
            "dt_interval": "835 days 12:30:15",
            "dt_timestamptz": datetime.datetime(2023, 10, 15, 11, 30, 45),
            "dt_timetz": datetime.timedelta(seconds=52245 - (3 * 60 * 60)),
        }
        description_dict = {row[0]: {"type_code": row[1], "flags": row[-2]} for row in description}
        row = res[0]
        for column_name, expected_type in expected_types.items():
            column_description = description_dict[column_name]

            if column_name == "t_bytea" and isinstance(row[column_name], (bytearray, bytes)):
                row[column_name] = row[column_name].decode()
            elif column_name == "dt_date" and isinstance(row[column_name], datetime.datetime):
                # NOTE sometime mysql.connector returns datetime instead of date for dt_date. This is suspicious, but ok
                assert row[column_name].hour == 0
                assert row[column_name].minute == 0
                assert row[column_name].second == 0
                row[column_name] = row[column_name].date()
            elif column_name in ("t_json", "t_jsonb") and self.use_binary:
                # NOTE 'binary' protocol returns json as bytearray.
                # by some reason, if use pytest then result is bytes instead of bytearray, but that is ok
                row[column_name] = row[column_name].decode()

            if isinstance(expected_values[column_name], float):
                assert abs(row[column_name] - expected_values[column_name]) < 1e-5, (
                    f"Expected value {expected_values[column_name]} for column {column_name}, but got {row[column_name]}, use_binary={self.use_binary}, table_name={table_name}"
                )
            elif column_name in ("t_json", "t_jsonb"):
                assert json.loads(row[column_name]) == json.loads(expected_values[column_name]), (
                    f"Expected value {expected_values[column_name]} for column {column_name}, but got {row[column_name]}, use_binary={self.use_binary}, table_name={table_name}"
                )
            else:
                assert row[column_name] == expected_values[column_name], (
                    f"Expected value {expected_values[column_name]} for column {column_name}, but got {row[column_name]}, use_binary={self.use_binary}, table_name={table_name}"
                )
            assert column_description["type_code"] == expected_type.code, (
                f"Expected type {expected_type.code} for column {column_name}, but got {column_description['type_code']}, use_binary={self.use_binary}, table_name={table_name}"
            )

            if os.uname().sysname == "Darwin":
                # It seems that flags on macos may be modified by mysql.connector on the client side.
                continue
            assert column_description["flags"] == sum(expected_type.flags), (
                f"Expected flags {sum(expected_type.flags)} for column {column_name}, but got {column_description['flags']}, use_binary={self.use_binary}, table_name={table_name}"
            )

    def test_create_mariadb_datasources(self, use_binary):
        db_details = {
            "type": "mariadb",
            "connection_data": {
                "type": "mariadb",
                "host": "samples.mindsdb.com",
                "port": "3307",
                "user": "demo_user",
                "password": "demo_password",
                "database": "test_data",
            },
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
                "database": "public",
            },
        }
        self.create_database("test_demo_mysql", db_details)
        self.validate_database_creation("test_demo_mysql")

    def test_create_predictor(self, use_binary):
        self.query(f"DROP MODEL IF EXISTS {self.predictor_name};")
        # add file lock here
        self.query(
            f"CREATE MODEL {self.predictor_name} from test_demo_postgres (select * from home_rentals) PREDICT rental_price;"
        )
        self.check_predictor_readiness(self.predictor_name)

    def test_making_prediction(self, use_binary):
        _query = f"""
            SELECT rental_price, rental_price_explain
            FROM {self.predictor_name}
            WHERE number_of_rooms = 2 and sqft = 400 and location = 'downtown' and days_on_market = 2 and initial_price= 2500;
        """
        res = self.query(_query)
        assert "rental_price" in res and "rental_price_explain" in res, (
            f"error getting prediction from {self.predictor_name} - {res}"
        )

    @pytest.mark.parametrize("describe_attr", ["model", "features", "ensemble"])
    def test_describe_predictor_attrs(self, describe_attr, use_binary):
        self.query(f"describe mindsdb.{self.predictor_name}.{describe_attr};")

    @pytest.mark.parametrize(
        "query",
        [
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
            "show models;",
            "show function status where db = 'mindsdb';",
            "show procedure status where db = 'mindsdb';",
        ],
    )
    def test_service_requests(self, query, use_binary):
        self.query(query)

    def test_show_columns(self, use_binary):
        ret = self.query("""
            SELECT
                *
            FROM information_schema.columns
            WHERE table_name = 'home_rentals' and table_schema='test_demo_postgres'
        """)
        assert len(ret) == 8
        # TODO FIX STR->INT casting
        # assert sorted([x['ordinal_position'] for x in ret]) == list(range(1, 9))

        rental_price_column = next(x for x in ret if x["column_name"] == "rental_price")
        assert rental_price_column["data_type"] == "int"
        assert rental_price_column["column_type"] == "int"
        assert rental_price_column["original_type"] == "integer"
        assert rental_price_column["numeric_precision"] is not None

        location_column = next(x for x in ret if x["column_name"] == "location")
        assert location_column["data_type"] == "varchar"
        assert location_column["column_type"].startswith("varchar(")  # varchar(###)
        assert location_column["original_type"] == "character varying"
        assert location_column["numeric_precision"] is None
        assert location_column["character_maximum_length"] is not None
        assert location_column["character_octet_length"] is not None

    def test_train_model_from_files(self, use_binary):
        df = pd.DataFrame(
            {
                "x1": [x for x in range(100, 210)] + [x for x in range(100, 210)],
                "x2": [x * 2 for x in range(100, 210)] + [x * 3 for x in range(100, 210)],
                "y": [x * 3 for x in range(100, 210)] + [x * 2 for x in range(100, 210)],
            }
        )
        file_predictor_name = "predictor_from_file"
        self.upload_ds(df, self.file_datasource_name)
        self.verify_file_ds(self.file_datasource_name)

        self.query(f"DROP MODEL IF EXISTS mindsdb.{file_predictor_name};")
        # add file lock here
        _query = f"""
            CREATE MODEL mindsdb.{file_predictor_name}
            from files (select * from {self.file_datasource_name})
            predict y;
        """
        self.query(_query)
        self.check_predictor_readiness(file_predictor_name)

    def test_select_from_files(self, use_binary):
        _query = f"select * from files.{self.file_datasource_name};"
        self.query(_query)

    @pytest.mark.slow
    def test_ts_train_and_predict(self, subtests, use_binary):
        train_df = pd.DataFrame(
            {
                "gby": ["A" for _ in range(100, 210)] + ["B" for _ in range(100, 210)],
                "oby": [x for x in range(100, 210)] + [x for x in range(200, 310)],
                "x1": [x for x in range(100, 210)] + [x for x in range(100, 210)],
                "x2": [x * 2 for x in range(100, 210)] + [x * 3 for x in range(100, 210)],
                "y": [x * 3 for x in range(100, 210)] + [x * 2 for x in range(100, 210)],
            }
        )

        test_df = pd.DataFrame(
            {
                "gby": ["A" for _ in range(210, 220)] + ["B" for _ in range(210, 220)],
                "oby": [x for x in range(210, 220)] + [x for x in range(310, 320)],
                "x1": [x for x in range(210, 220)] + [x for x in range(210, 220)],
                "x2": [x * 2 for x in range(210, 220)] + [x * 3 for x in range(210, 220)],
                "y": [x * 3 for x in range(210, 220)] + [x * 2 for x in range(210, 220)],
            }
        )

        train_ds_name = "train_ts_file_ds"
        test_ds_name = "test_ts_file_ds"
        for df, ds_name in [(train_df, train_ds_name), (test_df, test_ds_name)]:
            self.upload_ds(df, ds_name)
            self.verify_file_ds(ds_name)

        params = [
            (
                "with_group_by_hor1",
                f"CREATE MODEL mindsdb.%s from files (select * from {train_ds_name}) PREDICT y ORDER BY oby GROUP BY gby WINDOW 10 HORIZON 1;",
                f"SELECT res.gby, res.y as PREDICTED_RESULT FROM files.{test_ds_name} as source JOIN mindsdb.%s as res WHERE source.gby= 'A' LIMIT 1;",
                1,
            ),
            (
                "no_group_by_hor1",
                f"CREATE MODEL mindsdb.%s from files (select * from {train_ds_name}) PREDICT y ORDER BY oby WINDOW 10 HORIZON 1;",
                f"SELECT res.gby, res.y as PREDICTED_RESULT FROM files.{test_ds_name} as source JOIN mindsdb.%s as res LIMIT 1;",
                1,
            ),
            (
                "with_group_by_hor2",
                f"CREATE MODEL mindsdb.%s from files (select * from {train_ds_name}) PREDICT y ORDER BY oby GROUP BY gby WINDOW 10 HORIZON 2;",
                f"SELECT res.gby, res.y as PREDICTED_RESULT FROM files.{test_ds_name} as source JOIN mindsdb.%s as res WHERE source.gby= 'A' LIMIT 2;",
                2,
            ),
            (
                "no_group_by_hor2",
                f"CREATE MODEL mindsdb.%s from files (select * from {train_ds_name}) PREDICT y ORDER BY oby WINDOW 10 HORIZON 2;",
                f"SELECT res.gby, res.y as PREDICTED_RESULT FROM files.{test_ds_name} as source JOIN mindsdb.%s as res LIMIT 2;",
                2,
            ),
        ]
        for predictor_name, create_query, select_query, res_len in params:
            # add file lock here
            with subtests.test(
                msg=predictor_name,
                predictor_name=predictor_name,
                create_query=create_query,
                select_query=select_query,
                res_len=res_len,
            ):
                self.query(f"DROP MODEL IF EXISTS mindsdb.{predictor_name};")
                self.query(create_query % predictor_name)
                self.check_predictor_readiness(predictor_name)
                res = self.query(select_query % predictor_name)
                assert len(res) == res_len, f"prediction result {res} contains more that {res_len} records"

    @pytest.mark.slow
    def test_tableau_queries(self, subtests, use_binary):
        test_ds_name = self.file_datasource_name
        predictor_name = "predictor_from_file"
        integration = "files"

        queries = [
            f"""
               SELECT TABLE_NAME,TABLE_COMMENT,IF(TABLE_TYPE='BASE TABLE', 'TABLE', TABLE_TYPE),
               TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES
               WHERE TABLE_SCHEMA = '{integration}'
                AND TABLE_TYPE='BASE TABLE' ORDER BY TABLE_SCHEMA, TABLE_NAME
            """,
            f"""
                SELECT SUM(1) AS `cnt__0B4A4E8BD11C48FFB4730D4D2C32191A_ok`,
                  max(`Custom SQL Query`.`x1`) AS `sum_height_ok`,
                  max(`Custom SQL Query`.`y`) AS `sum_length1_ok`
                FROM (
                  SELECT res.x1, res.y
                   FROM files.{test_ds_name} as source
                   JOIN mindsdb.{predictor_name} as res
                ) `Custom SQL Query`
                HAVING (COUNT(1) > 0)
            """,
            f"""
                SHOW FULL TABLES FROM {integration}
            """,
            """
                SELECT `table_name`, `column_name`
                FROM `information_schema`.`columns`
                WHERE `data_type`='enum' AND `table_schema`='views';
            """,
            """
                SHOW KEYS FROM `mindsdb`.`predictors`
            """,
            """
                show full columns from `predictors`
            """,
            """
                SELECT `table_name`, `column_name` FROM `information_schema`.`columns`
                 WHERE `data_type`='enum' AND `table_schema`='mindsdb'
            """,
            f"""
                SELECT `Custom SQL Query`.`x1` AS `height`,
                  `Custom SQL Query`.`y` AS `length1`
                FROM (
                   SELECT res.x1, res.y
                   FROM files.{test_ds_name} as source
                   JOIN mindsdb.{predictor_name} as res
                ) `Custom SQL Query`
                LIMIT 100
            """,
            f"""
            SELECT
              `Custom SQL Query`.`x1` AS `x1`,
              SUM(`Custom SQL Query`.`y2`) AS `sum_y2_ok`
            FROM (
               SELECT res.x1, res.y as y2
               FROM files.{test_ds_name} as source
               JOIN mindsdb.{predictor_name} as res
            ) `Custom SQL Query`
            GROUP BY 1
            """,
            f"""
            SELECT
              `Custom SQL Query`.`x1` AS `x1`,
              COUNT(DISTINCT TRUNCATE(`Custom SQL Query`.`y`,0)) AS `ctd_y_ok`
            FROM (
               SELECT res.x1, res.y
               FROM files.{test_ds_name} as source
               JOIN mindsdb.{predictor_name} as res
            ) `Custom SQL Query`
            GROUP BY 1
            """,
        ]
        for i, _query in enumerate(queries):
            with subtests.test(msg=i, _query=_query):
                self.query(_query)
