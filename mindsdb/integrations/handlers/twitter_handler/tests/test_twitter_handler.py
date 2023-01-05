import unittest
from mindsdb.integrations.handlers.twitter_handler.twitter_handler import TwitterHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


class TwitterHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "twitter_user_name": "elonmusk",
            "twitter_api_token": "AAAAAAAAAAAAAAAAAAAAAKkrbgEAAAAARPWLaxlFo0vUOGavorPuTCzk1lM%3D2GJWOiEM15n5csdO42sm244kzyw9I0jIXEYxt6HmR7H3ZAcGXA",
            "tweets_start_time": "2023-01-01T10:00:50Z",
            "tweets_end_time": "2023-02-01T10:00:50Z",
            "twitter_endpoint_name": "tweets"
        }
        cls.handler = TwitterHandler('test_twitter_handler', cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_native_query_select(self):
        query = "SELECT * FROM user_tweets LIMIT 10 "
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_2_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_3_get_columns(self):
        columns = self.handler.get_columns()
        assert columns.type is not RESPONSE_TYPE.ERROR


if __name__ == '__main__':
    unittest.main()
