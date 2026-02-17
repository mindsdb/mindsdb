import unittest
from mindsdb.integrations.handlers.strapi_handler.strapi_handler import StrapiHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class StrapiHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        connection_data = {
            'host': 'localhost',
            'port': '1337',
            'api_token': 'c56c000d867e95848c',
            'plural_api_ids': ['products', 'sellers']}
        cls.handler = StrapiHandler(name='myshop', connection_data=connection_data)

    def test_0_check_connection(self):
        # Ensure the connection is successful
        self.assertTrue(self.handler.check_connection())

    def test_1_get_table(self):
        assert self.handler.get_tables() is not RESPONSE_TYPE.ERROR

    def test_2_get_columns(self):
        assert self.handler.get_columns('products') is not RESPONSE_TYPE.ERROR

    def test_3_get_data(self):
        # Ensure that you can retrieve data from a table
        data = self.handler.native_query('SELECT * FROM products')
        assert data.type is not RESPONSE_TYPE.ERROR

    def test_4_get_data_with_condition(self):
        # Ensure that you can retrieve data with a condition
        data = self.handler.native_query('SELECT * FROM products WHERE id = 1')
        assert data.type is not RESPONSE_TYPE.ERROR

    def test_5_insert_data(self):
        # Ensure that data insertion is successful
        query = "INSERT INTO myshop.sellers (name, email, sellerid) VALUES ('Ram', 'ram@gmail.com', 'ramu4')"
        result = self.handler.native_query(query)
        self.assertTrue(result)

    def test_6_update_data(self):
        # Ensure that data updating is successful
        query = "UPDATE products SET name = 'test2' WHERE id = 1"
        result = self.handler.native_query(query)
        self.assertTrue(result)


if __name__ == '__main__':
    unittest.main()
