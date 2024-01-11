import unittest
from mindsdb.integrations.handlers.google_books_handler.google_books_handler import GoogleBooksHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class GoogleBooksHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "credentials": "C:\\Users\\panagiotis\\Desktop\\GitHub\\mindsdb\\mindsdb\\integrations\\handlers"
                               "\\google_books_handler\\credentials.json"
            }
        }
        cls.handler = GoogleBooksHandler('test_google_books_handler', **cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_2_select_volume_query(self):
        query = "SELECT summary FROM my_books.volumes WHERE q = 'Harry Potter'"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_3_select_bookshelves_query(self):
        query = "SELECT title FROM my_books.bookshelves WHERE shelf > 1"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_4_get_columns(self):
        columns = self.handler.get_columns('id')
        assert columns.type is not RESPONSE_TYPE.ERROR


if __name__ == '__main__':
    unittest.main()
