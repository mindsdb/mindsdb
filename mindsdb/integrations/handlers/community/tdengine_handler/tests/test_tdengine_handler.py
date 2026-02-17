import unittest
from mindsdb.integrations.handlers.tdengine_handler.tdengine_handler import TDEngineHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class TDEngineHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "token": '<token_for_cloud>',
            "url": "********.cloud.tdengine.com",
            "database": "temp"
        }
        cls.handler = TDEngineHandler('test_tdengine_handler', connection_data=cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_connect(self):
        assert self.handler.connect()

    def test_2_create_table(self):
        query = "CREATE Table `hari` USING `temp` (`id`) TAGS (0);"
        result = self.handler.query(query)
        assert result.type is not RESPONSE_TYPE.ERROR
        pass

    def test_3_insert(self):
        query = "INSERT INTO hari  VALUES (NOW, 12);"
        result = self.handler.query(query)
        assert result.type is not RESPONSE_TYPE.ERROR

    def test_4_native_query_select(self):
        query = "SELECT * FROM hari;"
        result = self.handler.query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_5_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is RESPONSE_TYPE.TABLE

    def test_6_get_columns(self):
        columns = self.handler.get_columns('hari')
        assert columns.type is not RESPONSE_TYPE.ERROR


if __name__ == '__main__':
    unittest.main()
