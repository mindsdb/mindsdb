import os
import unittest

from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.integrations.handlers.confluence_handler.confluence_handler import (
    ConfluenceHandler,
)


class ConfluenceHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        connection_data = {
            'url': os.getenv('CONFLUENCE_URL'),
            'username': os.getenv('CONFLUENCE_USERNAME'),
            'password': os.getenv('CONFLUENCE_PASSWORD'),
        }
        cls.handler = ConfluenceHandler(
            name='confluence_handler', connection_data=connection_data
        )

    def test_connect(self):
        self.handler.connect()

    def test_check_connection(self):
        self.handler.check_connection()

    def test_get_spaces(self):
        response = self.handler.native_query("SELECT * FROM confluence_data.spaces")
        assert response.type is not RESPONSE_TYPE.ERROR

    def test_get_pages(self):
        response = self.handler.native_query(
            "SELECT * FROM confluence_data.pages WHERE space='DEMO'"
        )
        assert response.type is not RESPONSE_TYPE.ERROR

    # TODO: Uncomment when `insert` native query is fixed.

    # def test_insert_page(self):
    #     query = '''
    #     INSERT INTO confluence_data.pages (space, title, body)
    #     VALUES
    #     ('DEMO', 'Test page', 'Test page body')
    #     '''
    #     response = self.handler.native_query(query)
    #     assert response.type is not RESPONSE_TYPE.ERROR

    def test_update_page(self):
        query = f'''
        UPDATE confluence_data.pages
        SET title='Title updated', body='Body updated'
        WHERE id={os.getenv('CONFLUENCE_TEST_PAGE_ID')}
        '''
        response = self.handler.native_query(query)
        assert response.type is not RESPONSE_TYPE.ERROR

    def test_delete_page(self):
        query = f'''
        DELETE FROM confluence_data.pages
        WHERE id={os.getenv('CONFLUENCE_TEST_PAGE_ID')}
        '''
        response = self.handler.native_query(query)
        assert response.type is not RESPONSE_TYPE.ERROR


if __name__ == '__main__':
    unittest.main()
