import unittest
from unittest.mock import Mock, patch
import pandas as pd
from mindsdb.integrations.handlers.chromadb_handler.chromadb_handler import ChromaDBHandler, TableField

class MockCondition:
    def __init__(self, column, op, value):
        self.column = column
        self.op = op
        self.value = value

class TestChromaHandler(unittest.TestCase):
    
    def setUp(self):
        self.handler = ChromaDBHandler(name='test_chroma', connection_data={}, handler_storage=Mock())

    # INSERT
    @patch('mindsdb.integrations.handlers.chromadb_handler.chromadb_handler.ChromaDBHandler.connect')
    def test_insert_calls_upsert(self, mock_connect):
        mock_client = Mock()
        mock_collection = Mock()
        mock_client.get_or_create_collection.return_value = mock_collection
        self.handler._client = mock_client
        self.handler.is_connected = True

        df = pd.DataFrame({
            TableField.CONTENT.value: ['Cat Photo'],
            TableField.EMBEDDINGS.value: [[0.9, 0.1, 0.1]],
            TableField.ID.value: ['img_1'],
            TableField.METADATA.value: [{'author': 'Sriram'}]
        })
        self.handler.insert('my_gallery', df)
        
        call_args = mock_collection.upsert.call_args[1]
        self.assertEqual(call_args['embeddings'], [[0.9, 0.1, 0.1]])

    # SELECT 
    @patch('mindsdb.integrations.handlers.chromadb_handler.chromadb_handler.ChromaDBHandler.disconnect')
    @patch('mindsdb.integrations.handlers.chromadb_handler.chromadb_handler.ChromaDBHandler.connect')
    def test_select_semantic_search(self, mock_connect, mock_disconnect):
        
        # Mock System
        mock_client = Mock()
        mock_collection = Mock()
        mock_client.get_collection.return_value = mock_collection
        
        self.handler._client = mock_client
        self.handler.is_connected = True

        # Mock Return Data
        mock_result = {
            'ids': [['id1']],
            'documents': [['Dog']],
            'metadatas': [[{}]],
            'embeddings': [[[0.1, 0.2]]],
            'distances': [[0.5]]
        }
        mock_collection.query.return_value = mock_result
        mock_collection.get.return_value = mock_result

        conditions = [
            MockCondition(column=TableField.CONTENT.value, op='=', value='Dog')
        ]

        self.handler.select('my_gallery', conditions=conditions)

        # Verification
        if not mock_collection.query.called:
            self.fail("CRITICAL: The handler used .get() (Exact Match) instead of .query() (Semantic Search)!")
            
        call_args = mock_collection.query.call_args[1]
        
        if 'query_texts' not in call_args:
            self.fail("CRITICAL: The handler called .query() but forgot 'query_texts'!")
            
        self.assertEqual(call_args['query_texts'], ['Dog'])

if __name__ == '__main__':
    unittest.main()