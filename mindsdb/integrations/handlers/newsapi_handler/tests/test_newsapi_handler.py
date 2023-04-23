import unittest
from mindsdb.integrations.handlers.newsapi_handler.newsapi_handler import NewsAPIHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE

class NewsApiHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "api_key": "82fa480335ce42c0aa3758cb0efe66be"
            }
        }
        cls.handler = NewsAPIHandler('test_newsapi_handler', **cls.kwargs)

    def test_0_connect(self):
        self.handler.connect()
    
    def test_1_check_connection(self):
        self.handler.check_connection()

    def test_4_select(self):
        # table = self.handler.get_table("article")
        res = self.handler.native_query('SELECT * FROM article WHERE query="google" AND language="fr" ORDER BY publishedAt LIMIT 20 ')
        assert res.type is RESPONSE_TYPE.TABLE

        
if __name__ == '__main__':
    unittest.main()