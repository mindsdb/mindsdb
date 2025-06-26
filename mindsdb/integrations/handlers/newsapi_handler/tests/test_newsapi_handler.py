import unittest

from newsapi.newsapi_exception import NewsAPIException

from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.integrations.handlers.newsapi_handler.newsapi_handler import NewsAPIHandler


class NewsApiHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {"api_key": "82fa480335ce42c0aa3758cb0efe66be"}
        }
        cls.handler = NewsAPIHandler("test_newsapi_handler", **cls.kwargs)

    def test_0_connect(self):
        self.handler.connect()

    def test_1_check_connection(self):
        self.handler.check_connection()

    def test_2_select(self):
        # table = self.handler.get_table("article")
        res = self.handler.native_query('SELECT * FROM article WHERE query="google"')
        assert res.type is RESPONSE_TYPE.TABLE

    def test_3_select(self):
        # table = self.handler.get_table("article")
        with self.assertRaises(NewsAPIException):
            self.handler.native_query("SELECT * FROM article")

    def test_4_select(self):
        # table = self.handler.get_table("article")
        with self.assertRaises(NewsAPIException):
            self.handler.native_query(
                'SELECT * FROM article WHERE query="google" AND sources="google.com"'
            )

    def test_5_select(self):
        # table = self.handler.get_table("article")
        res = self.handler.native_query(
            'SELECT * FROM article WHERE query="google" AND sources="abc-news"'
        )
        assert res.type is RESPONSE_TYPE.TABLE

    def test_6_select(self):
        # table = self.handler.get_table("article")
        self.handler.native_query(
            'SELECT * FROM article WHERE query="google" AND publishedAt >= "2023-03-23" AND  publishedAt <= "2023-04-23"'
        )

    def test_7_select(self):
        # table = self.handler.get_table("article")
        res = self.handler.native_query(
            'SELECT * FROM article WHERE query="google" LIMIT 78'
        )
        assert res.type is RESPONSE_TYPE.TABLE

    def test_8_select(self):
        # table = self.handler.get_table("article")
        res = self.handler.native_query(
            'SELECT * FROM article WHERE query="google" LIMIT 150'
        )
        assert res.type is RESPONSE_TYPE.TABLE

    def test_9_select(self):
        # table = self.handler.get_table("article")
        res = self.handler.native_query(
            'SELECT * FROM article WHERE query="google" ORDER BY publishedAt'
        )
        assert res.type is RESPONSE_TYPE.TABLE

    def test_10_select(self):
        # table = self.handler.get_table("article")
        with self.assertRaises(NotImplementedError):
            self.handler.native_query(
                'SELECT * FROM article WHERE query="google" ORDER BY query'
            )

    def test_11_select(self):
        # table = self.handler.get_table("article")
        res = self.handler.native_query(
            'SELECT * FROM article WHERE query="google" ORDER BY relevancy'
        )
        assert res.type is RESPONSE_TYPE.TABLE


if __name__ == "__main__":
    unittest.main()
