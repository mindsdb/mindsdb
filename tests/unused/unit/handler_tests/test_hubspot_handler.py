import os
import unittest

from mindsdb.integrations.handlers.hubspot_handler.hubspot_handler import HubspotHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class HubSpotHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "access_token": os.environ.get('ACCESS_TOKEN')
            }
        }
        cls.handler = HubspotHandler('test_hubspot_handler', **cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_2_select_companies_query(self):
        query = "SELECT * FROM test_hubspot_handler.companies"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_3_select_contacts_query(self):
        query = "SELECT * FROM test_hubspot_handler.contacts"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_4_select_deals_query(self):
        query = "SELECT * FROM test_hubspot_handler.deals"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE
