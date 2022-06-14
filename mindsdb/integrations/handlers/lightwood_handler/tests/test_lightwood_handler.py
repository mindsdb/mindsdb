import unittest

from lightwood.mixer import LightGBM

from mindsdb.utilities.config import Config
from mindsdb.integrations.lightwood_handler.lightwood_handler.lightwood_handler import LightwoodHandler
from mindsdb.integrations.mysql_handler.mysql_handler.mysql_handler import MySQLHandler


class MLflowHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.handler = LightwoodHandler('test_ludwig_handler')
        cls.config = Config()

        kwargs = {"connection_data": {
            "host": "localhost",
            "port": "3306",
            "user": "root",
            "password": "root",
            "database": "test",
            "ssl": False
        }}
        cls.sql_handler_name = 'test_handler'
        cls.data_handler = MySQLHandler(cls.sql_handler_name, **kwargs)

        cls.target_1 = 'rental_price'
        cls.data_table_name_1 = 'home_rentals_subset'
        cls.test_model_name_1 = 'test_lightwood_home_rentals'

        cls.target_2 = 'Traffic'
        cls.data_table_name_1 = 'arrivals'
        cls.test_model_name_1 = 'test_lightwood_arrivals'

        # todo: connect to global state instead
        MDB_CURRENT_HANDLERS = {
            data_handler_name: MySQLHandler(self.sql_handler_name, **{
                "host": "localhost",
                "port": "3306",
                "user": "root",
                "password": "root",
                "database": "test",
                "ssl": False
            })
        }
        cls.data_handler = MDB_CURRENT_HANDLERS[data_handler_name]
        assert data_handler.check_status().success

    def test_1_connect(self):
        self.handler.connect()

    def test_2_drop_predictor(self):
        model_name = 'lw_test_predictor'
        try:
            print('dropping predictor...')
            cls.native_query(f"DROP PREDICTOR {model_name}")
        except:
            print('failed to drop')
            pass

    def test_3_train_predictor(self):
        data_table_name = 'home_rentals_subset'
        target = 'rental_price'
        if model_name not in cls.get_tables():
            query = f"CREATE PREDICTOR {model_name} FROM {data_handler_name} (SELECT * FROM test.{data_table_name}) PREDICT {target}"
            cls.native_query(query)

    def test_4_retrain_predictor(self):
        query = f"RETRAIN {model_name}"  # try retrain syntax
        cls.native_query(query)

    def test_5_list_tables(self):
        print(cls.get_tables())

    def test_6_describe_table(self):
        print(cls.describe_table(f'{model_name}'))

    def test_7_query_predictor_single_where_condition(self):
        # try single WHERE condition
        query = f"SELECT target from {model_name} WHERE sqft=100"
        parsed = cls.parser(query, dialect=cls.dialect)
        predicted = cls.query(parsed)['data_frame']

    def test_8_query_predictor_multi_where_condition(self):
        # try multiple
        query = f"SELECT target from {model_name} WHERE sqft=100 AND number_of_rooms=2 AND number_of_bathrooms=1"
        parsed = cls.parser(query, dialect=cls.dialect)
        predicted = cls.query(parsed)['data_frame']

    def test_9_join_predictor_into_table(self):
        into_table = 'test_join_into_lw'
        query = f"SELECT tb.{target} as predicted, ta.{target} as truth, ta.sqft from {data_handler_name}.{data_table_name} AS ta JOIN {model_name} AS tb LIMIT 10"
        parsed = cls.parser(query, dialect=cls.dialect)
        predicted = cls.join(parsed, data_handler, into=into_table)

        # checks whether `into` kwarg does insert into the table or not
        q = f"SELECT * FROM {into_table}"
        qp = cls.parser(q, dialect='mysql')
        assert len(data_handler.query(qp)['data_frame']) > 0

        # try:
        #     data_handler.native_query(f"DROP TABLE test.{into_table}")
        # except:
        #     pass

        # try:
        #     cls.native_query(f"DROP PREDICTOR {model_name}")
        # except:
        #     pass

    def test_10_train_predictor_custom_jsonai(self):
        # Test 2: add custom JsonAi
        model_name = 'lw_test_predictor2'
        # try:
        #     cls.native_query(f"DROP PREDICTOR {model_name}")
        # except:
        #     pass

        if model_name not in cls.get_tables():
            using_str = 'model.args={"submodels": [{"module": "LightGBM", "args": {"stop_after": 12, "fit_on_dev": True}}]}'
            query = f'CREATE PREDICTOR {model_name} FROM {data_handler_name} (SELECT * FROM test.{data_table_name}) PREDICT {target} USING {using_str}'
            cls.native_query(query)

        m = load_predictor(cls.storage.get('models')[model_name], model_name)
        assert len(m.ensemble.mixers) == 1
        assert isinstance(m.ensemble.mixers[0], LightGBM)

    def test_11_train_ts_predictor(self):
        # Timeseries predictor
        model_name = 'lw_test_predictor3'
        target = 'Traffic'
        data_table_name = 'arrival'
        oby = 'T'
        gby = 'Country'
        window = 8
        horizon = 4

        model_name = 'lw_test_predictor3'
        # try:
        #     cls.native_query(f"DROP PREDICTOR {model_name}")
        # except:
        #     pass

        if model_name not in cls.get_tables():
            query = f'CREATE PREDICTOR {model_name} FROM {data_handler_name} (SELECT * FROM test.{data_table_name}) PREDICT {target} ORDER BY {oby} GROUP BY {gby} WINDOW {window} HORIZON {horizon}'
            cls.native_query(query)

        p = cls.storage.get('models')
        m = load_predictor(p[model_name], model_name)
        assert m.problem_definition.timeseries_settings.is_timeseries

    def test_11_join_predictor_ts_into(self):
        # get predictions from a time series model
        into_table = 'test_join_tsmodel_into_lw'
        query = f"SELECT tb.{target} as predicted, ta.{target} as truth, ta.{oby} from {data_handler_name}.{data_table_name} AS ta JOIN mindsdb.{model_name} AS tb ON 1=1 WHERE ta.{oby} > LATEST LIMIT 10"
        parsed = cls.parser(query, dialect=cls.dialect)
        predicted = cls.join(parsed, data_handler, into=into_table)

        # try:
        #     data_handler.native_query(f"DROP TABLE {into_table}")
        # except Exception as e:
        #     print(e)

        # TODO: bring all train+predict queries in mindsdb_sql test suite
