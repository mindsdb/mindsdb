import unittest
from mindsdb.api.executor.data_types.response_type import (
    RESPONSE_TYPE,
)
from mindsdb.integrations.handlers.cloud_spanner_handler.cloud_spanner_handler import (
    CloudSpannerHandler,
)


class CloudSpannerHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {'connection_data': {'database_id': 'example-db', 'instance_id': 'test-instance', 'project': 'your-project-id'}}
        cls.handler = CloudSpannerHandler('test_cloud_spanner_handler', **cls.kwargs)

    def test_0_connect(self):
        self.handler.connect()

    def test_1_check_connection(self):
        self.handler.check_connection()

    def test_2_create_table(self):
        res = self.handler.query('CREATE TABLE integers(i INT64) PRIMARY KEY (i)')
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_3_insert_into_table(self):
        res = self.handler.query('INSERT INTO integers (i) VALUES (42)')
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_4_select(self):
        res = self.handler.query('SELECT * FROM integers')
        assert res.type is RESPONSE_TYPE.TABLE

    def test_5_describe_table(self):
        res = self.handler.get_columns('integers')
        assert res.type is RESPONSE_TYPE.TABLE

    def test_6_drop_table(self):
        res = self.handler.query('DROP TABLE integers')
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_7_get_tables(self):
        res = self.handler.get_tables()
        assert res.type is not RESPONSE_TYPE.ERROR


if __name__ == '__main__':
    unittest.main()
