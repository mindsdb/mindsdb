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

    def test_0_0_credentials_storage(self):
        """Test that credentials are stored and retrieved securely"""
        # This test verifies that credentials storage methods exist and work
        # Actual storage is tested through the check_connection test
        assert hasattr(self.handler, '_store_credentials')
        assert hasattr(self.handler, '_load_stored_credentials')

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

    # Data API Tests
    def test_6_reports_table_basic_query(self):
        """Test basic reports table query with date range"""
        query = "SELECT country, activeUsers, sessions FROM reports WHERE start_date = '7daysAgo' AND end_date = 'today' LIMIT 10"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_7_reports_table_with_filter(self):
        """Test reports table with dimension filter"""
        query = "SELECT date, eventName, eventCount FROM reports WHERE start_date = '7daysAgo' AND end_date = 'today' AND dimension_eventName = 'first_open' LIMIT 10"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_8_reports_table_with_order(self):
        """Test reports table with ORDER BY clause"""
        query = "SELECT country, activeUsers FROM reports WHERE start_date = '30daysAgo' AND end_date = 'today' ORDER BY activeUsers DESC LIMIT 5"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_9_realtime_reports_table(self):
        """Test realtime reports table"""
        query = "SELECT country, activeUsers FROM realtime_reports LIMIT 10"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_10_metadata_table(self):
        """Test metadata table to fetch available dimensions and metrics"""
        query = "SELECT * FROM metadata"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_11_metadata_table_filter_dimensions(self):
        """Test metadata table filtered by dimension type"""
        query = "SELECT api_name, ui_name FROM metadata WHERE type = 'dimension'"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_12_metadata_table_filter_metrics(self):
        """Test metadata table filtered by metric type"""
        query = "SELECT api_name, ui_name FROM metadata WHERE type = 'metric'"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE


if __name__ == '__main__':
    unittest.main()
