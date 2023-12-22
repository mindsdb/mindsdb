import os
import unittest
from mindsdb.integrations.handlers.sharepoint_handler import Handler as SharepointHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class SharepointHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "clientId": os.environ.get('CLIENT_ID'),
                "clientSecret": os.environ.get('CLIENT_SECRET'),
                "tenantId": os.environ.get('TENANT_ID'),
            }
        }
        cls.handler = SharepointHandler('test_sharepoint_handler', **cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_2_select_sites_query(self):
        query = "SELECT * FROM test_sharepoint_handler.sites"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_3_select_lists_query(self):
        query = "SELECT * FROM test_sharepoint_handler.lists"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_4_select_siteColumns_query(self):
        query = "SELECT * FROM test_sharepoint_handler.siteColumns"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_5_select_listItems_query(self):
        query = "SELECT * FROM test_sharepoint_handler.listItems"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_6_get_columns(self):
        columns = self.handler.get_columns('test_sharepoint_handler.siteColumns')
        assert columns.type is not RESPONSE_TYPE.ERROR


if __name__ == '__main__':
    unittest.main()
