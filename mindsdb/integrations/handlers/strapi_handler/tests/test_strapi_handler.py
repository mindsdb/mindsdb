import unittest
from mindsdb.integrations.handlers.strapi_handler.strapi_handler import StrapiHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


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
        self.assertTrue(self.handler.check_connection())

    def test_1_get_table(self):
        assert self.handler.get_tables() is not RESPONSE_TYPE.ERROR

    def test_2_get_columns(self):
        assert self.handler.get_columns('products') is not RESPONSE_TYPE.ERROR

    def test_3_select(self):
        self.assertTrue(self.handler.native_query('SELECT * FROM products'))

    def test_4_select_with_condition(self):
        self.assertTrue(self.handler.native_query('SELECT * FROM products WHERE id = 1'))

    def test_5_insert(self):
        query = "INSERT INTO myshop.sellers (name, email, sellerid) VALUES ('Ram', 'ram@gmail.com', 'ramu4')"
        self.assertTrue(self.handler.native_query(query))

    def test_6_update_data(self):
        query = "UPDATE products SET name = 'test2' WHERE id = 1"
        self.assertTrue(self.handler.native_query(query))


if __name__ == '__main__':
    unittest.main()
