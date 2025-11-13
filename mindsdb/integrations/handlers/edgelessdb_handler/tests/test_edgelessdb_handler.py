import unittest
from mindsdb.integrations.handlers.edgelessdb_handler.edgelessdb_handler import EdgelessDBHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class EdgelessDBHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "host": "localhost",
                "port": 8080,
                "user": "root",
                "password": "password",
                "database": "test",
            }
        }
        cls.handler = EdgelessDBHandler('test_edgelessdb_handler', **cls.kwargs)

    def test_0_connect(self):
        assert self.handler.connect()

    def test_1_drop_table(self):
        res = self.handler.query("DROP TABLE IF EXISTS TEST_TABLE")
        assert res.type is RESPONSE_TYPE.OK

    def test_2_create_table(self):
        res = self.handler.query(
            '''CREATE TABLE TEST_TABLE (
                ID INT PRIMARY KEY,
                NAME VARCHAR(14)
                )'''
        )
        assert res.type is RESPONSE_TYPE.OK

    def test_3_insert(self):
        res = self.handler.query(
            """INSERT INTO TEST_TABLE
            VALUES
                (100,'ONE HUNDRED'),
                (200,'TWO HUNDRED'),
                (300,'THREE HUNDRED')"""
        )
        assert res.type is RESPONSE_TYPE.OK

    def test_4_select(self):
        res = self.handler.query('SELECT * FROM TEST_TABLE')
        assert res.type is RESPONSE_TYPE.TABLE

    def test_5_check_connection(self):
        assert self.handler.check_connection()

    def test_6_get_tables(self):
        res = self.handler.get_tables()
        assert res.type is RESPONSE_TYPE.TABLE

    def test_7_get_columns(self):
        res = self.handler.get_columns("TEST_TABLE")
        assert res.type is RESPONSE_TYPE.TABLE

    def test_8_disconnect(self):
        assert self.handler.disconnect()


if __name__ == '__main__':
    unittest.main()
