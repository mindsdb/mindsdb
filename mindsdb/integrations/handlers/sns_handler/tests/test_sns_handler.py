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
        topic_name = "test12333"
        self.handler.create_topic({"name": topic_name})
        query = "SELECT * FROM topics"
        ast = parse_sql(query)
        sql_output = str(self.handler.query(ast))
        assert topic_name in sql_output

    def test_create_topic_and_select_by_topic_name(self):
        expected_topic_name = "test"
        self.handler.create_topic({"name": expected_topic_name})
        assert expected_topic_name in self.handler.call_sns_api("topic_list", {"topic_name": expected_topic_name})

    def test_publish_message(self):
        # todo add topic creation
        expected_topic_name = "arn:aws:sns:us-east-1:000000000000:aaaaaaa"
        response = self.handler.call_sns_api("publish_message", {"topic_arn": expected_topic_name, "message": "Test_message" })
        print(str(response))
    def test_publish_batch(self):
        request_entries = []
        expected_topic_name = "arn:aws:sns:us-east-1:000000000000:aaaaaaa"
        request_entry = {'Id': '2333334', 'Message': 'test', 'Subject': 'subject',
                         'MessageDeduplicationId': '1234556', 'MessageGroupId': '9999',
                       }
        request_entries.append(request_entry)
        # TopicArn=params['topic_arn'],
        #  PublishBatchRequestEntries=params['batch_request_entries']
        response=self.handler.call_sns_api("publish_batch", {"topic_arn": expected_topic_name, "batch_request_entries": request_entries})
        print(str(response))

# todo test duplicates batch empty respone
