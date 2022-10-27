import os
import unittest

temp_dir = '/tmp/ludwig_handler_test_pzgaf7ky'
os.environ['MINDSDB_STORAGE_DIR'] = temp_dir
os.environ['MINDSDB_DB_CON'] = 'sqlite:///' + os.path.join(os.environ['MINDSDB_STORAGE_DIR'], 'mindsdb.sqlite3.db') + '?check_same_thread=False&timeout=30'

from mindsdb.migrations import migrate
migrate.migrate_to_head()

from mindsdb.utilities.config import Config
from mindsdb.integrations.utilities.test_utils import HandlerControllerMock, PG_HANDLER_NAME
from mindsdb.integrations.handlers.ludwig_handler.ludwig_handler import LudwigHandler
from mindsdb.interfaces.storage.fs import ModelStorage, HandlerStorage
from mindsdb.integrations.handlers.mysql_handler.mysql_handler import MySQLHandler


# TODO: improve tests


class LudwigHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.handler = LudwigHandler(ModelStorage(None, None), HandlerStorage(None, None))
        cls.config = Config()

        cls.target_1 = 'rental_price'
        cls.data_table_1 = 'demo_data.home_rentals'
        cls.test_model_1 = 'test_ludwig_home_rentals'

        kwargs = {
            "host": "localhost",
            "port": "3306",
            "user": "root",
            "password": "root",
            "database": "test",
            "ssl": False
        }
        cls.sql_handler_name = 'test_data_handler'
        cls.data_table_name = 'train_escaped_csv'
        cls.data_handler = MySQLHandler(cls.sql_handler_name, **kwargs)

    def test_01_train_predictor(self):
        query = f"""
            CREATE PREDICTOR ludwig.{self.test_model_1}
            FROM {PG_HANDLER_NAME} (SELECT * FROM {self.data_table_1} limit 50)
            PREDICT rental_price
        """
        resp = self.data_handler.native_query(query)
        self.handler.create('rental_price', resp.data_frame)

    def test_02_select_from(self):
        # ludwig needs input for all columns in the original training df
        query = f"SELECT rental_price from {self.test_model_name} WHERE sqft=200 AND number_of_bathrooms=2 AND number_of_rooms=2 AND neighborhood='westbrae' AND location='great' AND days_on_market=10"
        resp = self.data_handler.native_query(query)
        self.handler.predict(resp.data_frame)
