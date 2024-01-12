import unittest
from mindsdb.integrations.handlers.aerospike_handler.aerospike_handler import AerospikeHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class AerospikeHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            # "user": "",
            # "password": "",
            "host": '172.17.0.2',
            "port": 3000,
            "namespace": "test",
        }
        cls.handler = AerospikeHandler('test_aerospike_handler', cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_native_query_select(self):
        query = "SELECT * FROM house_rentals"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_2_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_3_get_columns(self):
        columns = self.handler.get_columns('house_rentals')
        assert columns.type is not RESPONSE_TYPE.ERROR


if __name__ == '__main__':
    unittest.main()
