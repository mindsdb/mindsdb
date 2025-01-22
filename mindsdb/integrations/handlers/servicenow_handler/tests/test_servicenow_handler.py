import unittest
from unittest.mock import MagicMock
from mindsdb.integrations.handlers.servicenow_handler import ServiceNowHandler

class TestServiceNowHandler(unittest.TestCase):
    def setUp(self):
        self.config = {
            'instance': 'dev12345',
            'user': 'admin',
            'password': 'password'
        }
        self.handler = ServiceNowHandler(self.config)

    def test_validate_connection(self):
        self.handler.client.ping = MagicMock(return_value=True)
        self.assertTrue(self.handler.validate_connection())

    def test_fetch_data(self):
        mock_response = [{'sys_id': '123', 'short_description': 'Test Incident'}]
        self.handler.client.query_records = MagicMock(return_value=mock_response)
        result = self.handler.fetch_data({'table': 'incident', 'conditions': {}})
        self.assertEqual(result, mock_response)

    def test_push_data(self):
        mock_response = {'sys_id': '123'}
        self.handler.client.insert_record = MagicMock(return_value=mock_response)
        result = self.handler.push_data('incident', {'short_description': 'Test Incident'})
        self.assertEqual(result, mock_response)
