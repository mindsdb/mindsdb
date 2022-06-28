import unittest

from mindsdb.utilities.config import Config
from mindsdb.integrations.handlers.ludwig_handler.ludwig_handler import LudwigHandler
from mindsdb.integrations.handlers.mysql_handler.mysql_handler import MySQLHandler


class LudwigHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.handler = LudwigHandler('test_ludwig_handler')
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
        cls.target = 'rental_price'
        cls.data_table_name = 'home_rentals_subset'
        cls.data_handler = MySQLHandler(cls.sql_handler_name, **kwargs)
        cls.test_model_name = 'test_ludwig_home_rentals'

    def test_1_connect(self):
        self.assertTrue(
            self.handler.connect(config={
                    'path': self.config['paths']['root'],
                    'name': 'test_name'
                }).success
        )

    def test_2_drop_predictor(self):
        try:
            self.handler.native_query(f"DROP PREDICTOR {self.test_model_name}")
        except:
            target = 'rental_price'
            query = f"CREATE PREDICTOR {self.test_model_name} FROM {self.sql_handler_name} (SELECT * FROM test.{self.data_table_name}) PREDICT {self.target}"  # noqa
            self.handler.native_query(query)
            self.test_2_drop_predictor()

    def test_3_create_predictor(self):
        if self.test_model_name in self.handler.get_tables().data_frame['model_name']:
            print('dropping...')
            self.handler.native_query(f"DROP PREDICTOR {self.test_model_name}")
        query = f"CREATE PREDICTOR {self.test_model_name} FROM {self.sql_handler_name} (SELECT * FROM test.{self.data_table_name}) PREDICT {self.target}"  # noqa
        self.handler.native_query(query)

    def test_4_get_tables(self):
        self.handler.get_tables()

    def test_5_get_columns(self):
        self.handler.get_columns(f'{self.test_model_name}')

    def test_6_select_from(self):
        self.assertTrue(self.data_handler.check_connection())
        # ludwig needs input for all columns in the original training df
        query = f"SELECT rental_price from {self.test_model_name} WHERE sqft=200 AND number_of_bathrooms=2 AND number_of_rooms=2 AND neighborhood='westbrae' AND location='great' AND days_on_market=10"
        parsed = self.handler.parser(query, dialect=self.handler.dialect)
        predicted = self.handler.query(parsed)

    def test_7_join(self):
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
