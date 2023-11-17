import unittest
from mindsdb.integrations.handlers.sns_handler.sns_handler import SnsHandler
from mindsdb_sql import parse_sql
class SnsHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        #aws_access_key_id=self.connection_data['aws_access_key_id'],
        #        aws_secret_access_key=self.connection_data['aws_secret_access_key'],
        #        region_name=self.connection_data['region_name'],
        connection_data = {
            "aws_access_key_id": "PCAQ2LJDOSWLNSQKOCPW",
            "aws_secret_access_key": "U/VjewPlNopsDmmwItl34r2neyC6WhZpUiip57i",
            "region_name": "us-east-1",
            "endpoint_url":"http://localhost:4566"
            
        }
        cls.handler = SnsHandler('test_sns_handler', connection_data)
    #def test_0_check_connection(self):
    #    respone=self.handler.check_connection()
    #    assert respone.success is True
    def test_create_topic(self):
        topic_name="test"
        self.handler.create_topic(name=topic_name)
        #result = self.handler.query("SELECT * FROM TOPICS")
        query = "SELECT * FROM topics"
        ast = parse_sql(query)
        sql_output = str(self.handler.query(ast))
        assert "topic_name" in sql_output
            