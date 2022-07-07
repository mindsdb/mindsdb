import unittest

from mindsdb.integrations.handlers.ckan_handler.ckan_handler import CkanHandler
from mindsdb.utilities.config import Config


class CkanHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.handler = CkanHandler('test_ckan_handler')
        #cls.config = Config()

    def test_connect(self):
        kwargs = {"url": "https://www.data.qld.gov.au/"}
        self.handler.connect(**kwargs)

    def test_query(self):
        self.handler.query("ok")