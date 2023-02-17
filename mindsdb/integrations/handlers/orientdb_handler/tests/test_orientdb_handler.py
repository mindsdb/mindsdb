import unittest
from mindsdb.integrations.handlers.orientdb_handler.orientdb_handler import OrientDBHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


class MysqlHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
            "host": '192.168.10.128',
            "port": 2424,
            "user": 'root',
           "password": '123456',
            "database": 'test'
         }
        }
        cls.handler = OrientDBHandler('test_orientdb_handler', **cls.kwargs)

    # def test(self):
    #     self.handler.connect()
    #     # res = connetion.command("select from peoples1")
    #     # for i in range(0, len(res)):
    #     #     print(res[i])
    #     self.handler.disconnect()

    def test_0_check_connection(self):
        o = self.handler.check_connection()
        print()
        print(o.success)

    # def test_1_native_query_select(self):
    #     query = "SELECT * FROM ouser"
    #     result = self.handler.native_query(query)
    #     print()
    #     print(result.data_frame)
    #
    def test_2_get_tables(self):
        tables = self.handler.get_tables()
        print()
        print(tables.data_frame)
    #
    # def test_3_get_columns(self):
    #     columns = self.handler.get_columns('students')
    #     print(columns)


if __name__ == '__main__':
    unittest.main()
