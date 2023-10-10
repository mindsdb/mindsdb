import unittest
from unittest.mock import patch
from mindsdb.integrations.handlers.mediawiki_handler.mediawiki_handler import MediaWikiHandler
from mindsdb.integrations.handlers.mediawiki_handler.mediawiki_tables import PagesTable
from mindsdb_sql.parser import ast


class TestMediaWikiHandler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.handler = MediaWikiHandler("test_mediawiki_handler", "username", "password")
        cls.pages_table = PagesTable(cls.handler)

    @patch.object(PagesTable, 'get_pages')
    @patch.object(MediaWikiHandler, 'call_mediawiki_api')
    def test_update(self, mock_call_mediawiki_api, mock_get_pages):
        mock_get_pages.return_value = [
            {
                "pageid": 1,
                "title": "Old Title",
                "original_title": "Old Title",
                "content": "Old Content",
                "summary": "Old Summary",
                "url": "https://www.mediawiki.org/wiki/Old_Title",
                "categories": ["Category1", "Category2"]
            }
        ]
        mock_call_mediawiki_api.return_value = 200
        update_query = ast.Update(
            table='pages',
            update_columns={
                'title':'New Title',
                'content':'New Content',
                'summary':'New Summary'
            },
            where=[
                ('=', 'pageid', 1)
            ]
        )

        result = self.pages_table.update(update_query)

        mock_call_mediawiki_api.assert_called_once_with(
            "edit",
            {
                "title": "New Title",
                "content": "New Content",
                "summary": "New Summary",
            },
        )
        self.assertEqual(result.iloc[0]['title'], 'New Title')
        self.assertEqual(result.iloc[0]['content'], 'New Content')
        self.assertEqual(result.iloc[0]['summary'], 'New Summary')

if __name__ == "__main__":
    unittest.main()