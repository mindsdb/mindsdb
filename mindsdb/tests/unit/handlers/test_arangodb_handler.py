import unittest
from unittest.mock import MagicMock, patch
from mindsdb.integrations.handlers.arangodb_handler.arangodb_handler import ArangoDBHandler
from mindsdb.integrations.libs.response import RESPONSE_TYPE

class TestArangoDBHandler(unittest.TestCase):
    def setUp(self):
        self.connection_data = {
            "hosts": "http://localhost:8529",
            "database": "_system",
            "username": "root",
            "password": "password"
        }
        self.handler = ArangoDBHandler("test_arango", self.connection_data)

    def test_connect(self):
        with patch.object(self.handler, 'client') as mock_client:
            db_mock = MagicMock()
            self.handler.client = MagicMock()
            self.handler.client.db.return_value = db_mock
            
            # Since connect() instantiates ArangoClient, we need to mock it in the module or side-effect
            with patch("mindsdb.integrations.handlers.arangodb_handler.arangodb_handler.ArangoClient") as MockClient:
                instance = MockClient.return_value
                instance.db.return_value = db_mock
                
                db = self.handler.connect()
                self.assertEqual(db, db_mock)
                self.assertTrue(self.handler.is_connected)

    def test_native_query(self):
        with patch("mindsdb.integrations.handlers.arangodb_handler.arangodb_handler.ArangoClient") as MockClient:
            db_mock = MockClient.return_value.db.return_value
            # cursor returns list of dicts
            db_mock.aql.execute.return_value = [{"col1": "val1"}, {"col1": "val2"}]
            
            response = self.handler.native_query("RETURN 1")
            self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
            self.assertEqual(len(response.data_frame), 2)
            self.assertEqual(response.data_frame.iloc[0]["col1"], "val1")

    def test_get_tables(self):
        with patch("mindsdb.integrations.handlers.arangodb_handler.arangodb_handler.ArangoClient") as MockClient:
            db_mock = MockClient.return_value.db.return_value
            db_mock.collections.return_value = [
                {"name": "users", "system": False},
                {"name": "_graphs", "system": True}
            ]
            
            response = self.handler.get_tables()
            self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
            self.assertEqual(len(response.data_frame), 1)
            self.assertEqual(response.data_frame.iloc[0]["table_name"], "users")

if __name__ == '__main__':
    unittest.main()
