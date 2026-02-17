import unittest

from mindsdb.integrations.handlers.solr_handler.solr_handler import SolrHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class SolrHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "username": "demo_user",
            "password": "demo_password",
            "host": "172.22.0.4",
            "port": 8983,
            "server_path": "solr",
            "collection": "gettingstarted",
            "use_ssl": False
        }
        cls.handler = SolrHandler('test_solr_handler', **cls.kwargs)

    def test_0_connect(self):
        self.handler.check_connection()

    def test_2_get_tables(self):
        tbls = self.handler.get_tables()
        assert tbls['type'] is not RESPONSE_TYPE.ERROR

    def test_6_describe_table(self):
        described = self.handler.get_columns("gettingstarted")
        assert described['type'] is RESPONSE_TYPE.TABLE

    def test_7_select_query(self):
        query = "SELECT * FROM gettingstarted WHERE id='apple' limit 1000"
        result = self.handler.query(query)
        assert result['type'] is RESPONSE_TYPE.TABLE


if __name__ == '__main__':
    unittest.main()
