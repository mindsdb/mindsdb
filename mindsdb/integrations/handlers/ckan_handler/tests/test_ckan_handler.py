import unittest

from mindsdb.integrations.handlers.ckan_handler.ckan_handler import CkanHandler


class CkanHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.handler = CkanHandler('test_ckan_handler')
        cls.kwargs = {
            "connection_data": {
                "url": "http://demo.ckan.org/",
                "api_key": "demo"}
        }

    def test_connect(self):
        self.handler.connect()

    def test_query(self):
        self.handler.query("SELECT * from 'b53c9e72-6b59-4cda-8c0c-7d6a51dad12a'")
