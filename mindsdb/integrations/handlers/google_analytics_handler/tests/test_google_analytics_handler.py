from mindsdb.integrations.handlers.google_analytics_handler.google_analytics_handler import GoogleAnalyticsHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
import unittest


class GoogleAnalyticsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "credentials_file": "/home/talaat/Downloads/credentials.json",
                "property_id": '371809744'
            }
        }

        cls.handler = GoogleAnalyticsHandler('test_google_analytics_handler', **cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is RESPONSE_TYPE.TABLE


if __name__ == '__main__':
    unittest.main()
