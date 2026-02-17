import unittest
from mindsdb.integrations.handlers.openstreetmap_handler.openstreetmap_handler import OpenStreetMapHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class OpenStreetMapHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.handler = OpenStreetMapHandler(name='test_handler', connection_data={})

    def test_0_connect(self):
        assert self.handler.connect()

    def test_1_check_connection(self):
        assert self.handler.check_connection()

    def test_2_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_3_select_query(self):
        query = "SELECT * FROM openstreetmap_datasource.nodes WHERE id = 1;"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_4_native_query(self):
        query = "SELECT * FROM openstreetmap_datasource.nodes WHERE area = 'New Delhi';"
        response = self.handler.native_query(query)
        assert response.type is RESPONSE_TYPE.ERROR

    def test_5_disconnect(self):
        assert self.handler.disconnect()


if __name__ == '__main__':
    unittest.main()
