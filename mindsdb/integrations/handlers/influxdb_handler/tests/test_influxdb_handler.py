import unittest
from mindsdb.integrations.handlers.influxdb_handler.influxdb_handler import InfluxDBHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


class InfluxDBHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "influxdb_url": "https://ap-southeast-2-1.aws.cloud2.influxdata.com",
            "influxdb_token": "2KdXsJPE0yGpwxm6ybELC9-VjHYLN32QDTcNpRZUOVlgFJqJC7aUSLKcl26YwFP9_9jHqoih7FUWIy_zaIxfuw==",
            "influxdb_query": "SELECT * FROM airSensors Limit 10",
            "influxdb_db_name": "mindsdb",
            "influxdb_table_name": "airSensors"
        }
        cls.handler = InfluxDBHandler('test_influxdb_handler', cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_native_query_select(self):
        query = "SELECT * FROM airSensors LIMIT 10"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_2_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_3_get_columns(self):
        columns = self.handler.get_columns()
        assert columns.type is not RESPONSE_TYPE.ERROR


if __name__ == '__main__':
    unittest.main()
