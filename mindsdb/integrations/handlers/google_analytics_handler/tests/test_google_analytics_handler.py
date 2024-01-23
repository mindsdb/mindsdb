from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.integrations.handlers.google_analytics_handler.google_analytics_handler import GoogleAnalyticsHandler

import unittest


class GoogleAnalyticsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "credentials_file": '/home/talaat/Downloads/credentials.json',
                "property_id": '<YOUR_PROPERTY_ID>'
            }
        }

        cls.handler = GoogleAnalyticsHandler('test_google_analytics_handler', **cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is RESPONSE_TYPE.TABLE

    def test_2_native_query_select(self):
        query = 'SELECT * FROM conversion_events'
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_3_native_query_update(self):
        query = 'UPDATE conversion_events SET countingMethod = 1 WHERE name = "properties/371809744/conversionEvents/6637248600"'
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.OK

    def test_4_native_query_delete(self):
        query = 'DELETE FROM conversion_events WHERE name = "properties/371809744/conversionEvents/6622916665"'
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.OK

    def test_5_native_query_insert(self):
        query = "INSERT INTO conversion_events (event_name, countingMethod) VALUES ('event_4', 2)"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.OK


if __name__ == '__main__':
    unittest.main()
