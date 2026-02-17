from mindsdb.integrations.handlers.gmail_handler.gmail_handler import GmailHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
import unittest


class EmailsTableTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "credentials_file": "mindsdb/integrations/handlers/gmail_handler/credentials.json",
                "scopes": ['https://www.googleapis.com/auth/gmail.readonly']
            }
        }

        cls.handler = GmailHandler('test_gmail_handler', **cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_native_query_select(self):
        query = 'SELECT * FROM emails WHERE query = "alert from:*@google.com"'
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_2_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_3_get_columns(self):
        columns = self.handler.emails.get_columns()

        expected_columns = [
            'id',
            'message_id',
            'thread_id',
            'label_ids',
            'sender',
            'to',
            'date',
            'subject',
            'snippet',
            'body',
        ]

        self.assertListEqual(columns, expected_columns)


if __name__ == '__main__':
    unittest.main()
