import unittest
from mindsdb.integrations.handlers.google_content_shopping_handler.google_content_shopping_handler import \
    GoogleContentShoppingHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class GoogleSearchConsoleHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "merchant_id": "1234567890",
                "credentials": "/path/to/credentials.json"
            },
            "file_storage": "/path/to/file_storage"
        }
        cls.handler = GoogleContentShoppingHandler('test_google_content_shopping_handler', **cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_2_select_accounts_query(self):
        query = "SELECT * FROM accounts LIMIT 10"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_3_select_orders_query(self):
        query = "SELECT kind FROM orders WHERE orderId > 100"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_4_select_products_query(self):
        query = "SELECT price FROM products WHERE brand = 'Google' LIMIT 100"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_5_delete_accounts_query(self):
        query = "DELETE FROM accounts WHERE accountId = '1234567890'"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_6_delete_orders_query(self):
        query = "DELETE FROM orders WHERE orderId < '1234567890'"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_7_delete_products_query(self):
        query = "DELETE FROM products WHERE brand = 'Google'"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_8_update_products_query(self):
        query = "UPDATE products SET price = 100 WHERE productId > 120 AND updateMask = 'price'"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE


if __name__ == '__main__':
    unittest.main()
