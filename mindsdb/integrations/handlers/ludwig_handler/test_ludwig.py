import os
import unittest

temp_dir = '/tmp/ludwig_handler_test_pzgaf7ky'
os.environ['MINDSDB_STORAGE_DIR'] = temp_dir
os.environ['MINDSDB_DB_CON'] = 'sqlite:///' + os.path.join(os.environ['MINDSDB_STORAGE_DIR'], 'mindsdb.sqlite3.db') + '?check_same_thread=False&timeout=30'

from mindsdb.migrations import migrate
migrate.migrate_to_head()

from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage.fs import FsStore
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.utilities.with_kwargs_wrapper import WithKWArgsWrapper
from mindsdb.interfaces.model.model_controller import ModelController
from mindsdb.integrations.utilities.test_utils import HandlerControllerMock, PG_HANDLER_NAME
from mindsdb.integrations.handlers.ludwig_handler.ludwig_handler import LudwigHandler


class LudwigHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        handler_controller = HandlerControllerMock()

        cls.handler = LudwigHandler(
            'test_ludwig_handler',
            handler_controller=handler_controller,
            fs_store=WithKWArgsWrapper(FsStore(), company_id=None),
            model_controller=WithKWArgsWrapper(ModelController(), company_id=None)
        )
        cls.config = Config()

        cls.target_1 = 'rental_price'
        cls.data_table_1 = 'demo_data.home_rentals'
        cls.test_model_1 = 'test_ludwig_home_rentals'

    def test_00_connect(self):
        conn = self.handler.check_connection()
        assert conn.success

    def test_01_list_tables(self):
        response = self.handler.get_tables()
        self.assertTrue(response.type == RESPONSE_TYPE.TABLE)

    def test_02_get_columns(self):
        response = self.handler.get_columns(f'{self.test_model_1}')
        self.assertTrue(response.type == RESPONSE_TYPE.TABLE)
        self.assertTrue('COLUMN_NAME' in response.data_frame.columns)
        self.assertTrue('DATA_TYPE' in response.data_frame.columns)

    def test_03_drop_predictor(self):
        if self.test_model_1 not in self.handler.get_tables().data_frame.values:
            # TODO: seems redundant because of test_02
            query = f"""
                CREATE PREDICTOR {self.test_model_1}
                FROM {PG_HANDLER_NAME} (SELECT * FROM {self.data_table_1} limit 50)
                PREDICT rental_price
            """
            self.handler.native_query(query)
        response = self.handler.native_query(f"DROP PREDICTOR {self.test_model_1}")
        self.assertTrue(response.type == RESPONSE_TYPE.OK)

    def test_04_train_predictor(self):
        query = f"""
            CREATE PREDICTOR ludwig.{self.test_model_1}
            FROM {PG_HANDLER_NAME} (SELECT * FROM {self.data_table_1} limit 50)
            PREDICT rental_price
        """
        response = self.handler.native_query(query)
        self.assertTrue(response.type == RESPONSE_TYPE.OK)

    def test_05_select_from(self):
        self.assertTrue(self.data_handler.check_connection())
        # ludwig needs input for all columns in the original training df
        query = f"SELECT rental_price from {self.test_model_name} WHERE sqft=200 AND number_of_bathrooms=2 AND number_of_rooms=2 AND neighborhood='westbrae' AND location='great' AND days_on_market=10"
        parsed = self.handler.parser(query, dialect=self.handler.dialect)
        predicted = self.handler.query(parsed)

    def test_06_join(self):
        self.assertTrue(self.data_handler.check_connection())

        into_table = 'test_ludwig_join_into'
        query = f"SELECT tb.rental_price as predicted, ta.* from {self.sql_handler_name}.{self.data_table_name} AS ta JOIN mindsdb.{self.test_model_name} as tb LIMIT 10"

        parsed = self.handler.parser(query, dialect=self.handler.dialect)
        predicted = self.handler.join(parsed, self.data_handler, into=into_table)

        # TODO: restore
        # q = f"SELECT * FROM {into_table}"
        # qp = self.handler.parser(q, dialect='mysql')
        # assert len(self.data_handler.query(qp)) > 0
        #
        # try:
        #     self.data_handler.native_query(f"DROP TABLE test.{into_table}")
        # except:
        #     pass


if __name__ == "__main__":
    unittest.main(failfast=True)