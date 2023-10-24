import unittest

# from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from mindsdb.integrations.handlers.iceberg_handler.iceberg_handler import IcebergHandler


class IcebergHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        connection_data = {
            'name': 'demo',
            'namespace': 'docs_example',
            'user': 'postgres',
            'password': 'root',
            'database': 'demo',
            'table': 'bids',
        }
        cls.handler = IcebergHandler('test_iceberg_handler', connection_data)

    def test_0_connect(self):
        self.handler.connect()

    def test_1_check_connection(self):
        self.handler.check_connection()


if __name__ == "__main__":
    unittest.main()
