import unittest
from unittest.mock import Mock, patch
import pandas as pd
from mindsdb_sql.parser import ast
from mindsdb_sql import parse_sql
from mindsdb.integrations.handlers.zotero_handler.zotero_handler import ZoteroHandler
from mindsdb.integrations.handlers.zotero_handler.zotero_tables import AnnotationsTable


class AnnotationsTableTest(unittest.TestCase):
    def setUp(self):
        self.api_handler = Mock(ZoteroHandler)
        self.annotations_table = AnnotationsTable(self.api_handler)

    def test_get_columns_returns_all_columns(self):
        expected_columns = [
            'annotationColor', 'annotationComment', 'annotationPageLabel', 'annotationText',
            'annotationType', 'dateAdded', 'dateModified', 'key', 'parentItem',
            'relations', 'tags', 'version'
        ]
        self.assertListEqual(self.annotations_table.get_columns(), expected_columns)

    @patch.object(AnnotationsTable, '_get_items')
    def test_select_returns_all_columns(self, mock_get_items):
        mock_get_items.return_value = pd.DataFrame([
            {
                'annotationColor': 'red',
                'annotationComment': 'comment',
                'annotationPageLabel': 'page1',
                'annotationText': 'text',
                'annotationType': 'highlight',
                'dateAdded': '2023-01-01',
                'dateModified': '2023-01-02',
                'key': '12345',
                'parentItem': '67890',
                'relations': {},
                'tags': [],
                'version': 1
            }
        ])

        select_all = ast.Select(
            targets=[ast.Star()],
            from_table='annotations'
        )

        result = self.annotations_table.select(select_all)
        first_row = result.iloc[0]

        self.assertEqual(result.shape[1], 12)
        self.assertEqual(first_row['annotationColor'], 'red')
        self.assertEqual(first_row['annotationComment'], 'comment')
        self.assertEqual(first_row['annotationPageLabel'], 'page1')
        self.assertEqual(first_row['annotationText'], 'text')
        self.assertEqual(first_row['annotationType'], 'highlight')
        self.assertEqual(first_row['dateAdded'], '2023-01-01')
        self.assertEqual(first_row['dateModified'], '2023-01-02')
        self.assertEqual(first_row['key'], '12345')
        self.assertEqual(first_row['parentItem'], '67890')
        self.assertEqual(first_row['relations'], {})
        self.assertEqual(first_row['tags'], [])
        self.assertEqual(first_row['version'], 1)

    @patch.object(AnnotationsTable, '_get_item')
    def test_select_with_conditions_item_id(self, mock_get_item):
        mock_get_item.return_value = pd.DataFrame([
            {
                'annotationColor': 'blue',
                'annotationComment': 'another comment',
                'annotationPageLabel': 'page2',
                'annotationText': 'another text',
                'annotationType': 'underline',
                'dateAdded': '2023-03-01',
                'dateModified': '2023-03-02',
                'key': '54321',
                'parentItem': '09876',
                'relations': {},
                'tags': [],
                'version': 2
            }
        ])

        select_query = parse_sql('SELECT * FROM annotations WHERE item_id = "12345"')

        result = self.annotations_table.select(select_query)
        first_row = result.iloc[0]

        self.assertEqual(result.shape[1], 12)
        self.assertEqual(first_row['annotationColor'], 'blue')
        self.assertEqual(first_row['annotationComment'], 'another comment')
        self.assertEqual(first_row['annotationPageLabel'], 'page2')
        self.assertEqual(first_row['annotationText'], 'another text')
        self.assertEqual(first_row['annotationType'], 'underline')
        self.assertEqual(first_row['dateAdded'], '2023-03-01')
        self.assertEqual(first_row['dateModified'], '2023-03-02')
        self.assertEqual(first_row['key'], '54321')
        self.assertEqual(first_row['parentItem'], '09876')
        self.assertEqual(first_row['relations'], {})
        self.assertEqual(first_row['tags'], [])
        self.assertEqual(first_row['version'], 2)

    @patch.object(AnnotationsTable, '_get_item_children')
    def test_select_with_conditions_parent_item_id(self, mock_get_item_children):
        mock_get_item_children.return_value = pd.DataFrame([
            {
                'annotationColor': 'green',
                'annotationComment': 'yet another comment',
                'annotationPageLabel': 'page3',
                'annotationText': 'yet another text',
                'annotationType': 'strikeout',
                'dateAdded': '2023-05-01',
                'dateModified': '2023-05-02',
                'key': '98765',
                'parentItem': '43210',
                'relations': {},
                'tags': [],
                'version': 3
            }
        ])

        select_query = parse_sql('SELECT * FROM annotations WHERE parent_item_id = "67890"')

        result = self.annotations_table.select(select_query)
        first_row = result.iloc[0]

        self.assertEqual(result.shape[1], 12)
        self.assertEqual(first_row['annotationColor'], 'green')
        self.assertEqual(first_row['annotationComment'], 'yet another comment')
        self.assertEqual(first_row['annotationPageLabel'], 'page3')
        self.assertEqual(first_row['annotationText'], 'yet another text')
        self.assertEqual(first_row['annotationType'], 'strikeout')
        self.assertEqual(first_row['dateAdded'], '2023-05-01')
        self.assertEqual(first_row['dateModified'], '2023-05-02')
        self.assertEqual(first_row['key'], '98765')
        self.assertEqual(first_row['parentItem'], '43210')
        self.assertEqual(first_row['relations'], {})
        self.assertEqual(first_row['tags'], [])
        self.assertEqual(first_row['version'], 3)


class ZoteroHandlerTest(unittest.TestCase):

    @patch('pyzotero.zotero.Zotero')
    def setUp(self, mock_zotero):
        self.mock_zotero = mock_zotero
        connection_data = {
            'library_id': 'test_lib_id',
            'library_type': 'user',
            'api_key': 'test_api_key'
        }
        self.handler = ZoteroHandler(connection_data=connection_data)
        self.handler.connect()

    def test_connect(self):
        self.handler.connect()
        self.mock_zotero.assert_called_once_with(
            'test_lib_id', 'user', 'test_api_key'
        )
        self.assertTrue(self.handler.is_connected)

    def test_check_connection_success(self):
        self.handler.connect = Mock(return_value=None)
        self.handler.is_connected = True
        response = self.handler.check_connection()
        self.assertTrue(response.success)

    def test_check_connection_failure(self):
        self.handler.connect = Mock(side_effect=Exception('Connection failed'))
        response = self.handler.check_connection()
        self.assertFalse(response.success)
        self.assertIn('Error connecting to Zotero API', response.error_message)


if __name__ == '__main__':
    unittest.main()
