import unittest
from mindsdb.integrations.handlers.instatus_handler.instatus_handler import InstatusHandler, StatusPages
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
import pandas as pd


class InstatusHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.handler = InstatusHandler(name='mindsdb_instatus', connection_data={'api_key': 'd25509b171ad79395dc2c51b099ee6d0'})
        cls.table = StatusPages(cls.handler)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_call_instatus_api(self):
        self.assertIsInstance(self.handler.call_instatus_api(endpoint='/v2/pages'), pd.DataFrame)

    def test_2_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_3_get_columns(self):
        columns = self.table.get_columns()
        assert type(columns) is not RESPONSE_TYPE.ERROR


if __name__ == '__main__':
    unittest.main()
