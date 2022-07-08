import unittest

from lightwood.mixer import LightGBM

from mindsdb.utilities.config import Config
from mindsdb.integrations.handlers.lightwood_handler.lightwood_handler.lightwood_handler import LightwoodHandler
from mindsdb.integrations.handlers.lightwood_handler.lightwood_handler.utils import load_predictor
from mindsdb.integrations.handlers.mysql_handler.mysql_handler import MySQLHandler


# TODO: bring all train+predict queries in mindsdb_sql test suite
# TODO: drop all models and tables when closing tests
class LightwoodHandlerTest(unittest.TestCase):

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
        cls.test_model_name_1b = 'test_lightwood_home_rentals_custom'

        cls.target_2 = 'Traffic'
        cls.data_table_name_2 = 'arrival'
        cls.test_model_name_2 = 'test_lightwood_arrivals'
        cls.model_2_into_table = 'test_join_tsmodel_into_lw'

        # todo: connect to global state instead
        cls.MDB_CURRENT_HANDLERS = {
            cls.sql_handler_name: MySQLHandler(cls.sql_handler_name, **{"connection_data": {
                "host": "localhost",
                "port": "3306",
                "user": "root",
                "password": "root",
                "database": "test",
                "ssl": False
            }})
        }
        cls.data_handler = cls.MDB_CURRENT_HANDLERS[cls.sql_handler_name]

    def connect_handler(self):
        config = Config()
        self.handler.connect(config={'path': config['paths']['root'], 'name': 'lightwood_handler.db'})

    def test_0_connect(self):
        self.connect_handler()

    def test_1_drop_predictor(self):
        model_name = 'lw_test_predictor'
        try:
            print('dropping predictor...')
            self.handler.native_query(f"DROP PREDICTOR {model_name}")
        except:
            print('failed to drop')
            pass

    def test_21_train_predictor(self):
        if self.test_model_name_1 not in self.handler.get_tables():
            query = f"CREATE PREDICTOR {self.test_model_name_1} FROM {self.sql_handler_name} (SELECT * FROM test.{self.data_table_name_1}) PREDICT {self.target_1}"
            self.handler.native_query(query)

    def test_22_retrain_predictor(self):
        query = f"RETRAIN {self.test_model_name_1}"
        self.handler.native_query(query)

    def test_31_list_tables(self):
        print(self.handler.get_tables())

    def test_32_describe_table(self):
        print(self.handler.describe_table(f'{self.test_model_name_1}'))

    def test_41_query_predictor_single_where_condition(self):
        query = f"SELECT target from {self.test_model_name_1} WHERE sqft=100"
        parsed = self.handler.parser(query, dialect=self.handler.dialect)
        predicted = self.handler.query(parsed)['data_frame']

    def test_42_query_predictor_multi_where_condition(self):
        query = f"SELECT target from {self.test_model_name_1} WHERE sqft=100 AND number_of_rooms=2 AND number_of_bathrooms=1"
        parsed = self.handler.parser(query, dialect=self.handler.dialect)
        predicted = self.handler.query(parsed)['data_frame']

    def test_51_join_predictor_into_table(self):
        into_table = 'test_join_into_lw'
        query = f"SELECT tb.{self.target_1} as predicted, ta.{self.target_1} as truth, ta.sqft from {self.sql_handler_name}.{self.data_table_name_1} AS ta JOIN {self.test_model_name_1} AS tb LIMIT 10"
        parsed = self.handler.parser(query, dialect=self.handler.dialect)
        predicted = self.handler.join(parsed, self.data_handler, into=into_table)

        # checks whether `into` kwarg does insert into the table or not
        q = f"SELECT * FROM {into_table}"
        qp = self.handler.parser(q, dialect='mysql')
        assert len(self.data_handler.query(qp).data_frame) > 0

    def test_23_train_predictor_custom_jsonai(self):
        if self.test_model_name_1b not in self.handler.get_tables():
            using_str = 'model.args={"submodels": [{"module": "LightGBM", "args": {"stop_after": 12, "fit_on_dev": True}}]}'
            query = f'CREATE PREDICTOR {self.test_model_name_1b} FROM {self.sql_handler_name} (SELECT * FROM test.{self.data_table_name_1}) PREDICT {self.target_1} USING {using_str}'
            self.handler.native_query(query)

        m = load_predictor(self.handler.storage.get('models')[self.test_model_name_1b], self.test_model_name_1b)
        assert len(m.ensemble.mixers) == 1
        assert isinstance(m.ensemble.mixers[0], LightGBM)

    def test_24_train_ts_predictor(self):
        target = 'Traffic'
        oby = 'T'
        gby = 'Country'
        window = 8
        horizon = 4

        if self.test_model_name_2 not in self.handler.get_tables():
            query = f'CREATE PREDICTOR {self.test_model_name_2} FROM {self.sql_handler_name} (SELECT * FROM test.{self.data_table_name_2}) PREDICT {target} ORDER BY {oby} GROUP BY {gby} WINDOW {window} HORIZON {horizon}'
            self.handler.native_query(query)

        p = self.handler.storage.get('models')
        m = load_predictor(p[self.test_model_name_2], self.test_model_name_2)
        assert m.problem_definition.timeseries_settings.is_timeseries

    def test_52_join_predictor_ts_into(self):
        self.connect_handler()
        target = 'Traffic'
        oby = 'T'
        query = f"SELECT tb.{target} as predicted, ta.{target} as truth, ta.{oby} from {self.sql_handler_name}.{self.data_table_name_2} AS ta JOIN mindsdb.{self.test_model_name_2} AS tb ON 1=1 WHERE ta.{oby} > LATEST LIMIT 10"
        parsed = self.handler.parser(query, dialect=self.handler.dialect)
        predicted = self.handler.join(parsed, self.data_handler, into=self.model_2_into_table)

