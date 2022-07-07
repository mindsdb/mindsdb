import unittest

from mindsdb.integrations.handlers.ckan_handler.ckan_handler import CkanHandler
from mindsdb.utilities.config import Config


class CkanHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.handler = CkanHandler('test_ckan_handler')
        #cls.config = Config()

    def test_connect(self):
        kwargs = {"url": "https://demo.ckan.org/"}
        self.handler.connect(**kwargs)

    def test_query(self):
        self.handler.query("SELECT * from 'b53c9e72-6b59-4cda-8c0c-7d6a51dad12a'")