import unittest
import boto3
from mindsdb.integrations.handlers.sns_handler.sns_handler import SnsHandler
from mindsdb_sql import parse_sql
from pandas import DataFrame
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


class SnsHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        connection_data = {
            "aws_access_key_id": "some_key",
            "aws_secret_access_key": "some_access_key",
            "region_name": "us-east-1",
            "endpoint_url": "http://localhost:4566"

        }
        cls.handler = SnsHandler('test_sns_handler', connection_data)

    def test_0_check_connection(self):
        response = self.handler.check_connection()
        assert response.success is True

    def test_create_topic(self):
        topic_name = "test12333"
        self.handler.create_topic({"name": topic_name})
        query = "SELECT * FROM topics"
        ast = parse_sql(query)
        sql_output = str(self.handler.query(ast))
        assert topic_name in sql_output

    def test_create_topic_and_select_by_topic_name(self):
        expected_topic_name = "test"
        self.handler.create_topic({"name": expected_topic_name})
        result=self.handler.call_sns_api("topic_list", {"name": expected_topic_name})
        assert expected_topic_name in str(result)
 
  
    def test_nagative_create_topic_and_select_by_topic_name(self):
        expected_topic_name = "test"
        self.handler.create_topic({"name": expected_topic_name})
        expected_topic_name = "random123445556"
        result=self.handler.call_sns_api("topic_list", {'name': expected_topic_name})
        assert str(result)=="[]"
     
  
    def test_create_topic_and_select_all_topics(self):
        expected_topic_name = "test"
        self.handler.create_topic({"name": expected_topic_name})
        response = self.handler.call_sns_api("topic_list", {})
        assert expected_topic_name in str(response)
    
 
    def test_publish_message(self):
        expected_topic_name = "test"
        df = self.handler.create_topic({"name": expected_topic_name})
        expected_topic_name_arn = df["TopicArn"][0]
        response = self.handler.call_sns_api("publish_message", {"topic_arn": expected_topic_name_arn, "message": "Test_message" })
        assert response["ResponseMetadata"][1] == 200 
   
    def test_publish_batch(self):
        request_entries = []
        expected_topic_name = "test"
        df = self.handler.create_topic({"name": expected_topic_name})
        expected_topic_name_arn = df["TopicArn"][0]
        request_entry = {'Id': '2333334', 'Message': 'test', 'Subject': 'subject',
                         'MessageDeduplicationId': '1234556', 'MessageGroupId': '9999',
                       }
        request_entries.append(request_entry)
        response =self.handler.call_sns_api("publish_batch", {"topic_arn": expected_topic_name_arn, "batch_request_entries": request_entries})
        assert len(response["MessageId"][0])>2
  
    def test_table(self):
        tables = self.handler.get_tables()       
        assert tables.type is not RESPONSE_TYPE.ERROR
    
  
    def test_publish_batch_which_contains_dupicate_ids(self):
        request_entries = []
        expected_topic_name = "test12"
        df = self.handler.create_topic({"name": expected_topic_name})
        expected_topic_name_arn=df["TopicArn"][0]
        request_entry = {'Id': '23333345', 'Message': 'test', 'Subject': 'subject',
                         'MessageDeduplicationId': '12345562', 'MessageGroupId': '9999',
                       }
        request_entry1 = {'Id': '2333334', 'Message': 'test1', 'Subject': 'subject1',
                         'MessageDeduplicationId': '12345561', 'MessageGroupId': '19999',
                       }
        request_entries.append(request_entry)
        request_entries.append(request_entry1)
        response = self.handler.call_sns_api("publish_batch", {"topic_arn": expected_topic_name_arn, "batch_request_entries": request_entries})
        assert len(response["MessageId"][0])>2