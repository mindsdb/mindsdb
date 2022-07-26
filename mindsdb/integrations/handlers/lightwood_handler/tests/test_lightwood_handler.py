import os
import unittest
import tempfile
import json
from pathlib import Path

from lightwood.mixer import LightGBM

os.environ['MINDSDB_STORAGE_DIR'] = tempfile.mkdtemp(dir='/tmp/', prefix='lightwood_handler_test_')
os.environ['MINDSDB_DB_CON'] = 'sqlite:///' + os.path.join(os.environ['MINDSDB_STORAGE_DIR'], 'mindsdb.sqlite3.db') + '?check_same_thread=False&timeout=30'

from mindsdb.migrations import migrate
migrate.migrate_to_head()

from mindsdb.utilities.config import Config
from mindsdb.integrations.handlers.lightwood_handler.lightwood_handler.lightwood_handler import LightwoodHandler
from mindsdb.integrations.handlers.lightwood_handler.lightwood_handler.utils import load_predictor
from mindsdb.integrations.handlers.mysql_handler.mysql_handler import MySQLHandler
from mindsdb.interfaces.storage.fs import FsStore
from mindsdb.interfaces.model.model_interface import ModelInterface
from mindsdb.utilities.with_kwargs_wrapper import WithKWArgsWrapper
from mindsdb.integrations.libs.response import RESPONSE_TYPE


MYSQL_CONNECTION_DATA = {
    "host": "localhost",
    "port": "3306",
    "user": "root",
    "password": "root",
    "database": "test",
    "ssl": False
}

with open(str(Path.home().joinpath('.mindsdb_credentials.json')), 'rt') as f:
    MYSQL_CONNECTION_DATA = json.loads(f.read())['mysql']

MYSQL_HANDLER_NAME = 'test_handler'


class HandlerControllerMock:
    def __init__(self):
        self.handlers = {
            MYSQL_HANDLER_NAME: MySQLHandler(
                MYSQL_HANDLER_NAME,
                **{"connection_data": MYSQL_CONNECTION_DATA}
            )
        }

    def get_handler(self, name):
        return self.handlers[name]

    def get(self, name):
        class Meta:
            id = 0
        return Meta()


# TODO: bring all train+predict queries in mindsdb_sql test suite
# TODO: drop all models and tables when closing tests
class LightwoodHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        handler_controller = HandlerControllerMock()

        cls.handler = LightwoodHandler(
            'lightwood',
            handler_controller=handler_controller,
            fs_store=WithKWArgsWrapper(FsStore(), company_id=None),
            model_controller=WithKWArgsWrapper(ModelInterface(), company_id=None)
        )
        cls.config = Config()

        cls.target_1 = 'rental_price'
        cls.data_table_name_1 = 'home_rentals_subset'
        cls.test_model_name_1 = 'test_lightwood_home_rentals'
        cls.test_model_name_1b = 'test_lightwood_home_rentals_custom'

        cls.target_2 = 'Traffic'
        cls.data_table_name_2 = 'arrival'
        cls.test_model_name_2 = 'test_lightwood_arrivals'
        cls.model_2_into_table = 'test_join_tsmodel_into_lw'

    def test_1_drop_predictor(self):
        model_name = 'lw_test_predictor'
        try:
            print('dropping predictor...')
            self.handler.native_query(f"DROP PREDICTOR {model_name}")
        except Exception as e:
            print(f'failed to drop: {e}')

    def test_21_train_predictor(self):
        # if self.test_model_name_1 not in self.handler.get_tables():
        query = f"""
            CREATE PREDICTOR {self.test_model_name_1}
            FROM {MYSQL_HANDLER_NAME} (SELECT * FROM test_data.home_rentals limit 50)
            PREDICT rental_price
        """
        response = self.handler.native_query(query)
        self.assertTrue(type(response) == RESPONSE_TYPE.OK)

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
        query = f"SELECT tb.{self.target_1} as predicted, ta.{self.target_1} as truth, ta.sqft from {MYSQL_HANDLER_NAME}.{self.data_table_name_1} AS ta JOIN {self.test_model_name_1} AS tb LIMIT 10"
        parsed = self.handler.parser(query, dialect=self.handler.dialect)
        predicted = self.handler.join(parsed, self.data_handler, into=into_table)

        # checks whether `into` kwarg does insert into the table or not
        q = f"SELECT * FROM {into_table}"
        qp = self.handler.parser(q, dialect='mysql')
        assert len(self.data_handler.query(qp).data_frame) > 0

    def test_23_train_predictor_custom_jsonai(self):
        if self.test_model_name_1b not in self.handler.get_tables():
            using_str = 'model.args={"submodels": [{"module": "LightGBM", "args": {"stop_after": 12, "fit_on_dev": True}}]}'
            query = f'CREATE PREDICTOR {self.test_model_name_1b} FROM {MYSQL_HANDLER_NAME} (SELECT * FROM test.{self.data_table_name_1}) PREDICT {self.target_1} USING {using_str}'
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
            query = f'CREATE PREDICTOR {self.test_model_name_2} FROM {MYSQL_HANDLER_NAME} (SELECT * FROM test.{self.data_table_name_2}) PREDICT {target} ORDER BY {oby} GROUP BY {gby} WINDOW {window} HORIZON {horizon}'
            self.handler.native_query(query)

        p = self.handler.storage.get('models')
        m = load_predictor(p[self.test_model_name_2], self.test_model_name_2)
        assert m.problem_definition.timeseries_settings.is_timeseries

    def test_52_join_predictor_ts_into(self):
        self.connect_handler()
        target = 'Traffic'
        oby = 'T'
        query = f"SELECT tb.{target} as predicted, ta.{target} as truth, ta.{oby} from {MYSQL_HANDLER_NAME}.{self.data_table_name_2} AS ta JOIN mindsdb.{self.test_model_name_2} AS tb ON 1=1 WHERE ta.{oby} > LATEST LIMIT 10"
        parsed = self.handler.parser(query, dialect=self.handler.dialect)
        predicted = self.handler.join(parsed, self.data_handler, into=self.model_2_into_table)


if __name__ == "__main__":
    unittest.main(failfast=True)