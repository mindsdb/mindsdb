import unittest
from mindsdb.integrations.handlers.databend_handler.databend_handler import DatabendHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class DatabendHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        connection_data = {
            "host": "some-url.aws-us-east-2.default.databend.com",
            "port": 443,
            "user": "root",
            "password": "password",
            "database": "test_db"
        }
        cls.handler = DatabendHandler('test_databend_handler', connection_data)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_select_query(self):
        query = 'SELECT * FROM covid_19_us_2022_4668 LIMIT 10'
        result = self.handler.query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_2_get_tables(self):
        tbls = self.handler.get_tables()
        assert tbls.type is not RESPONSE_TYPE.ERROR

    def test_3_describe_table(self):
        described = self.handler.get_columns("covid_19_us_2022_4668")
        print('described', described)
        assert described.type is RESPONSE_TYPE.TABLE


if __name__ == '__main__':
    unittest.main()
