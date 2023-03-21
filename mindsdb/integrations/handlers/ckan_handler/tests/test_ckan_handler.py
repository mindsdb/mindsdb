import unittest

from mindsdb.integrations.handlers.ckan_handler.ckan_handler import CkanHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


class CkanHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "url": "http://demo.ckan.org/"}
        }
        cls.handler = CkanHandler('test_ckan_handler', **cls.kwargs)

    def test_connect(self):
        self.handler.connect()

    def test_query(self):
        self.handler.query("SELECT * from 'b53c9e72-6b59-4cda-8c0c-7d6a51dad12a'")

    def test_disconnect(self):
        self.handler.disconnect()

    def test_get_tables(self):
        tables = self.handler.get_tables()
        assert tables['type'] is not RESPONSE_TYPE.ERROR
