import unittest
from mindsdb.integrations.handlers.sns_handler.sns_handler import SnsHandler
from mindsdb_sql import parse_sql


class SnsHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        connection_data = {
            "aws_access_key_id": "PCAQ2LJDOSWLNSQKOCPW",
            "aws_secret_access_key": "U/VjewPlNopsDmmwItl34r2neyC6WhZpUiip57i",
            "region_name": "us-east-1",
            "endpoint_url": "http://localhost:4566"

        }
        cls.handler = SnsHandler('test_sns_handler', connection_data)

    def test_0_check_connection(self):
        response = self.handler.check_connection()
        assert response.success is True

    def test_create_topic(self):
        topic_name = "test"
        self.handler.create_topic({"name": topic_name})
        query = "SELECT * FROM topics"
        ast = parse_sql(query)
        sql_output = str(self.handler.query(ast))
        assert topic_name in sql_output

    def test_create_topic_and_select_by_name(self):
        expected_topic_name = "test"
        self.handler.create_topic({"name": expected_topic_name})
        assert expected_topic_name in self.handler.call_sns_api("topic_list", {"topic_name": expected_topic_name})
