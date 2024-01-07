from mindsdb.integrations.handlers.google_analytics_handler.google_analytics_handler import GoogleAnalyticsHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
import unittest
from datetime import datetime as dt

PROPERTY_ID = 371809744
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

    def test_2_create_conversion_event(self):
        params = {
            'name': 'test',
            'event_name': 'talaat',
            'create_time': dt.now(),
            'deletable': True,
            'custom': True,
            'countingMethod': 1,
        }
        df = self.handler.create_conversion_event(params)
        print(df)

    def test_3_get_conversion_events(self):
        params = {
            'page_size': 100
        }
        df = self.handler.get_conversion_events(params)
        print(df)



if __name__ == '__main__':
    unittest.main()
