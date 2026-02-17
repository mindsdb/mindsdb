import unittest
from mindsdb.integrations.handlers.google_search_handler.google_search_handler import GoogleSearchConsoleHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class GoogleSearchConsoleHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "credentials": "/path/to/credentials.json"
            },
            "file_storage": "/path/to/file_storage"
        }
        cls.handler = GoogleSearchConsoleHandler('test_google_search_console_handler', **cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_2_select_analytics_query(self):
        query = "SELECT clicks FROM my_console.Analytics WHERE siteUrl = " \
                "'https://www.mindsdb.com' AND startDate = '2020-10-01' " \
                "AND endDate = '2020-10-31'"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_3_select_sitemaps_query(self):
        query = "SELECT type FROM my_console.Sitemaps WHERE siteUrl = " \
                "'https://www.mindsdb.com' AND feedpath = " \
                "'https://www.mindsdb.com/sitemap.xml'"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_4_insert_sitemaps_query(self):
        query = "INSERT INTO my_console.Sitemaps VALUES " \
                "('https://www.mindsdb.com/sitemap.xml'," \
                "'https://www.mindsdb')"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_5_delete_sitemaps_query(self):
        query = "DELETE FROM my_console.Sitemaps WHERE " \
                "siteUrl = 'https://www.mindsdb' AND feedpath = " \
                "'https://www.mindsdb.com/sitemap.xml'"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE


if __name__ == '__main__':
    unittest.main()
