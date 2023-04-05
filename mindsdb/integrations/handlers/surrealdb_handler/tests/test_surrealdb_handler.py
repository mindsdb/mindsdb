import unittest

from mindsdb.integrations.handlers.surrealdb_handler.surrealdb_handler import SurrealDBHandler


class SurrealdbHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "host": "localhost",
                "port": "8000",
                "user": "admin",
                "password": "password",
                "namespace": "test",
                "database": "test"
            }
        }
        cls.handler = SurrealDBHandler('test_surrealdb_handler', **cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_disconnect(self):
        assert self.handler.disconnect() is None


if __name__ == '__main__':
    unittest.main()
