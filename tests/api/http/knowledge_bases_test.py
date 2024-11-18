
import os
from tempfile import TemporaryDirectory


from unittest.mock import patch, MagicMock
import pytest

from mindsdb.api.http.initialize import initialize_app
from mindsdb.api.mysql.mysql_proxy.mysql_proxy import SQLAnswer
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.migrations import migrate
from mindsdb.interfaces.storage import db
from mindsdb.utilities.config import Config


@pytest.fixture(scope="session", autouse=True)
def app():
    old_minds_db_con = ''
    if 'MINDSDB_DB_CON' in os.environ:
        old_minds_db_con = os.environ['MINDSDB_DB_CON']
    with TemporaryDirectory(prefix='knowledge_bases_test_') as temp_dir:
        db_path = 'sqlite:///' + os.path.join(temp_dir, 'mindsdb.sqlite3.db')
        # Need to change env variable for migrate module, since it calls db.init().
        os.environ['MINDSDB_DB_CON'] = db_path
        db.init()
        migrate.migrate_to_head()
        app = initialize_app(Config(), True)

        test_client = app.test_client()

        # Create langchain embedding model to use in all tests.
        create_ml_engine_query = 'CREATE ML_ENGINE langchain_embedding FROM langchain_embedding;'
        create_ml_engine_data = {
            'query': create_ml_engine_query
        }
        response = test_client.post('/api/sql/query', json=create_ml_engine_data, follow_redirects=True)
        assert '200' in response.status

        # Create model to use in all tests.
        create_query = '''
        CREATE MODEL mindsdb.test_embedding_model
        PREDICT embeddings
        USING
            engine='langchain_embedding',
            class = 'FakeEmbeddings',
            size = 512,
            input_columns = ['content'];
        '''
        train_data = {
            'query': create_query
        }
        response = test_client.post('/api/projects/mindsdb/models', json=train_data, follow_redirects=True)
        assert '201' in response.status

        yield app

    os.environ['MINDSDB_DB_CON'] = old_minds_db_con


@pytest.fixture()
def client(app):
    return app.test_client()


def test_get_knowledge_base(client):
    get_knowledge_bases_response = client.get('/api/projects/mindsdb/knowledge_bases')

    assert len(get_knowledge_bases_response.get_json()) == 0

    create_request = {
        'knowledge_base': {
            'name': 'test_get_kb',
            'model': 'test_embedding_model'
        }
    }

    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)

    assert '201' in create_response.status

    get_knowledge_bases_response = client.get('/api/projects/mindsdb/knowledge_bases')

    assert '200' in get_knowledge_bases_response.status

    all_knowledge_bases = get_knowledge_bases_response.get_json()

    assert len(all_knowledge_bases) == 1

    get_knowledge_base_response = client.get('/api/projects/mindsdb/knowledge_bases/test_get_kb')

    assert '200' in get_knowledge_base_response.status

    created_knowledge_base = create_response.get_json()
    test_get_kb = get_knowledge_base_response.get_json()
    expected_create_knowledge_base = {
        'name': 'test_get_kb',
        'embedding_model': 'test_embedding_model',
        'vector_database': 'test_get_kb_chromadb',
        'vector_database_table': 'default_collection',
        'id': created_knowledge_base['id'],
        'project_id': created_knowledge_base['project_id'],
        'params': created_knowledge_base['params'],
        'created_at': created_knowledge_base['created_at'],
        'updated_at': created_knowledge_base['updated_at']
    }
    assert created_knowledge_base == expected_create_knowledge_base
    assert test_get_kb == expected_create_knowledge_base

    # Returned fields are slightly different for GET all vs POST.
    fetched_knowledge_base = all_knowledge_bases[0]
    expected_get_knowledge_base = {
        'name': 'test_get_kb',
        'embedding_model': 'test_embedding_model',
        'vector_database': 'test_get_kb_chromadb',
        'vector_database_table': 'default_collection',
        'id': created_knowledge_base['id'],
        'project_name': 'mindsdb',
        'project_id': created_knowledge_base['project_id'],
        'params': created_knowledge_base['params']
    }
    assert fetched_knowledge_base == expected_get_knowledge_base


def test_create_knowledge_base_no_storage(client):
    create_request = {
        'knowledge_base': {
            'name': 'test_kb',
            'model': 'test_embedding_model'
        }
    }

    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)

    assert '201' in create_response.status

    created_knowledge_base = create_response.get_json()
    expected_knowledge_base = {
        'name': 'test_kb',
        'embedding_model': 'test_embedding_model',
        'vector_database': 'test_kb_chromadb',
        'vector_database_table': 'default_collection',
        'id': created_knowledge_base['id'],
        'project_id': created_knowledge_base['project_id'],
        'params': created_knowledge_base['params'],
        'created_at': created_knowledge_base['created_at'],
        'updated_at': created_knowledge_base['updated_at']
    }
    assert created_knowledge_base == expected_knowledge_base


def test_create_knowledge_base_no_knowledge_base_param(client):
    create_request = {
        'name': 'test_kb',
        'model': 'test_embedding_model'
    }

    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)

    assert '400' in create_response.status


def test_create_knowledge_base_no_name(client):
    create_request = {
        'knowledge_base': {
            'model': 'test_embedding_model'
        }
    }

    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)

    assert '400' in create_response.status


def test_create_knowledge_base_no_model(client):
    create_request = {
        'knowledge_base': {
            'name': 'test_kb'
        }
    }

    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)

    assert '400' in create_response.status


def test_create_knowledge_base_no_storage_database(client):
    create_request = {
        'knowledge_base': {
            'name': 'test_kb',
            'model': 'test_embedding_model',
            'storage': {
                'table': 'vector_db_table'
            }
        }
    }

    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)

    assert '400' in create_response.status


def test_create_knowledge_base_no_storage_table(client):
    create_request = {
        'knowledge_base': {
            'name': 'test_kb',
            'model': 'test_embedding_model',
            'storage': {
                'database': 'vector_db'
            }
        }
    }

    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)

    assert '400' in create_response.status


def test_create_knowledge_base_project_not_found(client):
    create_request = {
        'knowledge_base': {
            'name': 'test_kb',
            'model': 'test_embedding_model'
        }
    }

    create_response = client.post('/api/projects/buoluobao/knowledge_bases', json=create_request, follow_redirects=True)

    assert '404' in create_response.status


def test_create_knowledge_base_already_exists(client):
    create_request = {
        'knowledge_base': {
            'name': 'test_kb_already_exists',
            'model': 'test_embedding_model'
        }
    }

    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)

    assert '201' in create_response.status

    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)

    assert '409' in create_response.status


def test_delete_knowledge_base(client):
    create_request = {
        'knowledge_base': {
            'name': 'test_delete_kb',
            'model': 'test_embedding_model'
        }
    }

    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)

    assert '201' in create_response.status

    delete_response = client.delete('/api/projects/mindsdb/knowledge_bases/test_delete_kb', follow_redirects=True)
    assert '204' in delete_response.status

    get_response = client.get('/api/projects/mindsdb/knowledge_bases/test_delete_kb', follow_redirects=True)
    assert '404' in get_response.status


def test_delete_knowledge_base_project_not_found(client):
    delete_response = client.delete('/api/projects/chasiubao/knowledge_bases/test_kb', follow_redirects=True)
    assert '404' in delete_response.status


def test_delete_knowledge_base_not_found(client):
    delete_response = client.delete('/api/projects/mindsdb/knowledge_bases/xiaolongbao_kb', follow_redirects=True)
    assert '404' in delete_response.status


def test_put_knowledge_base_rows(client):
    create_request = {
        'knowledge_base': {
            'name': 'test_kb_update_rows',
            'model': 'test_embedding_model'
        }
    }

    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)
    assert '201' in create_response.status

    content_to_embed = '''To begin with a perfect Peking duck recipe at home, firstly choose head on (easy to hang for air drying out), clean and leaner ducks.
    Add around 1 teaspoon of white vinegar in clean water and soak the duck for 1 hour. Then prepare lines and tie the ducks from the top of the neck.
    Hang them on hooks. I hang the ducks on the top of kitchen pool.
    Please note:  I make this peking duck in March when the room temperature is around 13-15 degree C, you will need to hang the duck in fridge or in a room with air conditioner in hot summer days.
    '''

    test_id = 0
    rows_to_insert = [
        {'id': test_id, 'content': content_to_embed}
    ]
    update_request = {
        'knowledge_base': {
            'rows': rows_to_insert
        }
    }

    with patch('mindsdb.interfaces.knowledge_base.controller.KnowledgeBaseTable') as mock_kb_table:
        # Create a mock KB instance
        mock_table_instance = MagicMock()
        mock_kb_table.return_value = mock_table_instance

        # Setup the _kb attribute with required params
        mock_kb = MagicMock()
        mock_kb.params = {'preprocessing': None}  # Initial state
        mock_table_instance._kb = mock_kb

        update_response = client.put(
            '/api/projects/mindsdb/knowledge_bases/test_kb_update_rows',
            json=update_request,
            follow_redirects=True
        )

        assert '200' in update_response.status

        # Verify insert_rows was called with correct data
        mock_table_instance.insert_rows.assert_called_once()
        actual_rows = mock_table_instance.insert_rows.call_args[0][0]
        assert len(actual_rows) == 1
        assert actual_rows[0]['id'] == test_id
        assert actual_rows[0]['content'] == content_to_embed


def test_put_knowledge_base_query(client):
    create_request = {
        'knowledge_base': {
            'name': 'test_kb_update_query',
            'model': 'test_embedding_model'
        }
    }
    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)
    assert '201' in create_response.status

    content_to_embed = '''To begin with a perfect Peking duck recipe at home, firstly choose head on (easy to hang for air drying out), clean and leaner ducks.
    Add around 1 teaspoon of white vinegar in clean water and soak the duck for 1 hour. Then prepare lines and tie the ducks from the top of the neck.
    Hang them on hooks. I hang the ducks on the top of kitchen pool.
    Please note:  I make this peking duck in March when the room temperature is around 13-15 degree C, you will need to hang the duck in fridge or in a room with air conditioner in hot summer days.
    '''
    update_request = {
        'knowledge_base': {
            'query': 'SELECT * FROM mock_db.recipes'
        }
    }

    # Mock the FakeMysqlProxy
    mock_proxy = MagicMock()
    mock_proxy.process_query.return_value = SQLAnswer(
        resp_type=RESPONSE_TYPE.TABLE,
        columns=[{'alias': 'id'}, {'name': 'content'}],
        data=[(0, content_to_embed)]
    )

    with patch('mindsdb.interfaces.knowledge_base.controller.KnowledgeBaseTable') as mock_kb_table:
        mock_table_instance = MagicMock()
        mock_kb_table.return_value = mock_table_instance

        # Setup the mock table
        mock_kb = MagicMock()
        mock_kb.params = {'preprocessing': None}
        mock_table_instance._kb = mock_kb
        mock_table_instance.mysql_proxy = mock_proxy

        update_response = client.put(
            '/api/projects/mindsdb/knowledge_bases/test_kb_update_query',
            json=update_request,
            follow_redirects=True
        )

        assert '200' in update_response.status

        # Verify insert was called with correct data
        mock_table_instance.insert_query_result.assert_called_once_with(
            'SELECT * FROM mock_db.recipes',
            'mindsdb'
        )


@pytest.fixture
def create_test_kb(client):
    kb_name = 'test_completions_kb'

    # First, try to delete the existing knowledge base if it exists
    client.delete(f'/api/projects/mindsdb/knowledge_bases/{kb_name}', follow_redirects=True)
    create_kb_request = {
        'knowledge_base': {
            'name': kb_name,
            'model': 'test_embedding_model'
        }
    }
    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_kb_request,
                                  follow_redirects=True)

    # Check if creation was successful
    assert create_response.status_code in [201,
                                           200], f"Failed to create knowledge base. Status: {create_response.status}"

    return kb_name


def test_successful_completion(client, create_test_kb):
    kb_name = create_test_kb
    completion_request = {
        'query': 'What is the capital of France?',
    }
    response = client.post(f'/api/projects/mindsdb/knowledge_bases/{kb_name}/completions',
                           json=completion_request, follow_redirects=True)
    assert response.status_code == 200
    response_data = response.get_json()
    assert 'message' in response_data
    assert 'content' in response_data['message']
    assert 'context' in response_data['message']
    assert response_data['message']['role'] == 'assistant'


def test_completion_missing_query_parameter(client, create_test_kb):
    kb_name = create_test_kb
    invalid_request = {
        'knowledge_base': kb_name
    }
    response = client.post(f'/api/projects/mindsdb/knowledge_bases/{kb_name}/completions',
                           json=invalid_request, follow_redirects=True)
    assert response.status_code == 400


def test_completion_non_existent_project(client, create_test_kb):
    kb_name = create_test_kb
    completion_request = {
        'query': 'What is the capital of France?',
    }
    response = client.post(f'/api/projects/nonexistent/knowledge_bases/{kb_name}/completions',
                           json=completion_request, follow_redirects=True)
    assert response.status_code == 404


def test_completion_non_existent_knowledge_base(client):
    completion_request = {
        'query': 'What is the capital of France?',
    }
    response = client.post('/api/projects/mindsdb/knowledge_bases/nonexistent_kb/completions',
                           json=completion_request, follow_redirects=True)
    assert response.status_code == 404


def test_create_knowledge_base_with_preprocessing(client):
    """Test creating a knowledge base with preprocessing configuration"""
    create_request = {
        'knowledge_base': {
            'name': 'test_kb_preprocess',
            'model': 'test_embedding_model',
            'preprocessing': {
                'type': 'contextual',
                'contextual_config': {
                    'chunk_size': 1000,
                    'chunk_overlap': 200,
                    'llm_model': 'gpt-4'
                }
            }
        }
    }

    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)
    assert '201' in create_response.status

    created_knowledge_base = create_response.get_json()
    assert 'preprocessing' in created_knowledge_base['params']
    assert created_knowledge_base['params']['preprocessing']['type'] == 'contextual'

    # Verify preprocessing config is preserved in GET
    get_response = client.get('/api/projects/mindsdb/knowledge_bases/test_kb_preprocess')
    assert '200' in get_response.status
    kb_data = get_response.get_json()
    assert 'preprocessing' in kb_data['params']
    assert kb_data['params']['preprocessing'] == create_request['knowledge_base']['preprocessing']


def test_create_knowledge_base_invalid_preprocessing(client):
    """Test creating a knowledge base with invalid preprocessing configuration"""
    create_request = {
        'knowledge_base': {
            'name': 'test_kb_invalid_preprocess',
            'model': 'test_embedding_model',
            'preprocessing': {
                'type': 'invalid_type',
                'contextual_config': {
                    'chunk_size': 'invalid'
                }
            }
        }
    }

    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)
    assert '400' in create_response.status


def test_put_knowledge_base_with_preprocessing(client):
    """Test updating knowledge base with preprocessing"""
    # First create a KB
    create_request = {
        'knowledge_base': {
            'name': 'test_kb_update_preprocess',
            'model': 'test_embedding_model'
        }
    }

    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)
    assert '201' in create_response.status

    # Update with content and preprocessing
    content_to_embed = [
        "First document to be processed and chunked.",
        "Second document with different content for testing.",
    ]
    rows_to_insert = [
        {'content': content_to_embed[0]},
        {'content': content_to_embed[1]}
    ]

    update_request = {
        'knowledge_base': {
            'rows': rows_to_insert,
            'preprocessing': {
                'type': 'contextual',
                'contextual_config': {
                    'chunk_size': 500,
                    'chunk_overlap': 100,
                    'llm_model': 'gpt-4'
                }
            }
        }
    }

    with patch('mindsdb.interfaces.knowledge_base.controller.KnowledgeBaseTable') as mock_kb_table:
        # Create a mock KB instance
        mock_table_instance = MagicMock()
        mock_kb_table.return_value = mock_table_instance

        # Setup the _kb attribute with required params
        mock_kb = MagicMock()
        mock_kb.params = {'preprocessing': None}  # Initial state
        mock_table_instance._kb = mock_kb

        update_response = client.put(
            '/api/projects/mindsdb/knowledge_bases/test_kb_update_preprocess',
            json=update_request,
            follow_redirects=True
        )

        assert '200' in update_response.status

        # Verify preprocessing was configured
        mock_table_instance.configure_preprocessing.assert_called_with({
            'type': 'contextual',
            'contextual_config': {
                'chunk_size': 500,
                'chunk_overlap': 100,
                'llm_model': 'gpt-4'
            }
        })

        # Verify rows were inserted
        mock_table_instance.insert_rows.assert_called_once()
        inserted_rows = mock_table_instance.insert_rows.call_args[0][0]
        assert len(inserted_rows) == 2
        assert inserted_rows[0]['content'] == content_to_embed[0]
        assert inserted_rows[1]['content'] == content_to_embed[1]


def test_put_knowledge_base_invalid_preprocessing(client):
    """Test updating knowledge base with invalid preprocessing config"""
    # First create a KB
    create_request = {
        'knowledge_base': {
            'name': 'test_kb_invalid_update_preprocess',
            'model': 'test_embedding_model'
        }
    }

    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)
    assert '201' in create_response.status

    # Update with invalid preprocessing config
    update_request = {
        'knowledge_base': {
            'rows': [{'content': 'test content'}],
            'preprocessing': {
                'type': 'invalid_type',
                'invalid_config': {
                    'invalid_param': 'invalid_value'
                }
            }
        }
    }

    with patch('mindsdb.interfaces.knowledge_base.controller.KnowledgeBaseTable') as mock_kb_table:
        # Create a mock KB instance
        mock_table_instance = MagicMock()
        mock_kb_table.return_value = mock_table_instance

        # Setup the _kb attribute with required params
        mock_kb = MagicMock()
        mock_kb.params = {'preprocessing': None}
        mock_table_instance._kb = mock_kb

        # Configure the mock to raise an error when invalid preprocessing config is provided
        mock_table_instance.configure_preprocessing.side_effect = ValueError("Invalid preprocessing type")

        update_response = client.put(
            '/api/projects/mindsdb/knowledge_bases/test_kb_invalid_update_preprocess',
            json=update_request,
            follow_redirects=True
        )

        assert '400' in update_response.status


def test_put_knowledge_base_with_documents(client):
    """Test updating knowledge base with Document objects"""
    create_request = {
        'knowledge_base': {
            'name': 'test_kb_documents',
            'model': 'test_embedding_model'
        }
    }

    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)
    assert '201' in create_response.status

    # Test data
    test_documents = [
        {
            'content': 'First test document content',
            'metadata': {'source': 'test1', 'category': 'A'}
        },
        {
            'content': 'Second test document content',
            'metadata': {'source': 'test2', 'category': 'B'}
        }
    ]

    update_request = {
        'knowledge_base': {
            'rows': test_documents
        }
    }

    with patch('mindsdb.interfaces.knowledge_base.controller.KnowledgeBaseTable') as mock_kb_table:
        mock_table_instance = MagicMock()
        mock_kb_table.return_value = mock_table_instance

        # Mock the dependencies
        mock_kb = MagicMock()
        mock_kb.params = {'preprocessing': None}
        mock_table_instance._kb = mock_kb

        update_response = client.put(
            '/api/projects/mindsdb/knowledge_bases/test_kb_documents',
            json=update_request,
            follow_redirects=True
        )

        assert '200' in update_response.status

        # Verify insert_rows was called with correct Document objects
        mock_table_instance.insert_rows.assert_called_once()
        inserted_rows = mock_table_instance.insert_rows.call_args[0][0]
        assert len(inserted_rows) == 2
        assert all(isinstance(row, dict) for row in inserted_rows)
        assert inserted_rows[0]['content'] == test_documents[0]['content']
        assert inserted_rows[0]['metadata'] == test_documents[0]['metadata']


def test_put_knowledge_base_mixed_content(client):
    """Test updating knowledge base with multiple content types"""
    create_request = {
        'knowledge_base': {
            'name': 'test_kb_mixed',
            'model': 'test_embedding_model'
        }
    }

    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)
    assert '201' in create_response.status

    update_request = {
        'knowledge_base': {
            'rows': [{'content': 'Test content', 'metadata': {'source': 'manual'}}],
            'files': ['test_file.txt'],
            'urls': ['http: //example.com'],
            'query': 'SELECT * FROM test_table',
            'preprocessing': {
                'type': 'contextual',
                'contextual_config': {
                    'chunk_size': 500,
                    'chunk_overlap': 50
                }
            }
        }
    }

    with patch('mindsdb.interfaces.knowledge_base.controller.KnowledgeBaseTable') as mock_kb_table:
        mock_table_instance = MagicMock()
        mock_kb_table.return_value = mock_table_instance

        # Mock the dependencies
        mock_kb = MagicMock()
        mock_kb.params = {'preprocessing': None}
        mock_table_instance._kb = mock_kb

        update_response = client.put(
            '/api/projects/mindsdb/knowledge_bases/test_kb_mixed',
            json=update_request,
            follow_redirects=True
        )

        assert '200' in update_response.status


def test_create_knowledge_base_with_text_chunking(client):
    """Test creating a knowledge base with text chunking preprocessing configuration"""
    create_request = {
        'knowledge_base': {
            'name': 'test_kb_text_chunking',
            'model': 'test_embedding_model',
            'preprocessing': {
                'type': 'text_chunking',
                'text_chunking_config': {
                    'chunk_size': 500,
                    'chunk_overlap': 50,
                    'separators': ["\n\n", "\n", ".", " "]
                }
            }
        }
    }

    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)
    assert '201' in create_response.status

    created_knowledge_base = create_response.get_json()
    assert 'preprocessing' in created_knowledge_base['params']
    assert created_knowledge_base['params']['preprocessing']['type'] == 'text_chunking'

    # Verify preprocessing config is preserved in GET
    get_response = client.get('/api/projects/mindsdb/knowledge_bases/test_kb_text_chunking')
    assert '200' in get_response.status
    kb_data = get_response.get_json()
    assert 'preprocessing' in kb_data['params']
    assert kb_data['params']['preprocessing'] == create_request['knowledge_base']['preprocessing']


def test_create_knowledge_base_invalid_text_chunking(client):
    """Test creating a knowledge base with invalid text chunking configuration"""
    create_request = {
        'knowledge_base': {
            'name': 'test_kb_invalid_chunking',
            'model': 'test_embedding_model',
            'preprocessing': {
                'type': 'text_chunking',
                'text_chunking_config': {
                    'chunk_size': -100,  # Invalid negative size
                    'chunk_overlap': "invalid",  # Invalid type
                }
            }
        }
    }

    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)
    assert '400' in create_response.status


def test_put_knowledge_base_default_preprocessing(client):
    """Test that text chunking is used as default preprocessing when none specified"""
    create_request = {
        'knowledge_base': {
            'name': 'test_kb_default_preprocess',
            'model': 'test_embedding_model'
        }
    }

    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)
    assert '201' in create_response.status

    update_request = {
        'knowledge_base': {
            'rows': [{
                'content': 'Test content for default preprocessing',
                'source': 'test'  # Flat metadata instead of nested
            }]
        }
    }

    # Send update request directly
    update_response = client.put(
        '/api/projects/mindsdb/knowledge_bases/test_kb_default_preprocess',
        json=update_request,
        follow_redirects=True
    )

    assert '200' in update_response.status


def test_put_knowledge_base_missing_metadata(client):
    """Test proper error handling when metadata is missing"""
    create_request = {
        'knowledge_base': {
            'name': 'test_kb_metadata',
            'model': 'test_embedding_model'
        }
    }

    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)
    assert '201' in create_response.status

    # Request with missing source
    update_request = {
        'knowledge_base': {
            'rows': [{
                'content': 'Test content without metadata'
            }]
        }
    }

    update_response = client.put(
        '/api/projects/mindsdb/knowledge_bases/test_kb_metadata',
        json=update_request,
        follow_redirects=True
    )

    assert '200' in update_response.status  # Should succeed with default metadata


def test_document_processing_with_valid_metadata(client):
    """Test document processing with valid metadata configuration"""
    create_request = {
        'knowledge_base': {
            'name': 'test_kb_valid_metadata',
            'model': 'test_embedding_model',
            'preprocessing': {
                'type': 'text_chunking',
                'text_chunking_config': {
                    'chunk_size': 100,
                    'chunk_overlap': 20
                }
            }
        }
    }

    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)
    assert '201' in create_response.status

    # Various valid metadata configurations as flat key-value pairs
    test_rows = [
        {
            'content': 'First test content',
            'source': 'test1',
            'category': 'A'
        },
        {
            'content': 'Second test content',
            'source': 'test2',
            'doc_type': 'document'
        },
        {
            'content': 'Third test content',
            'source': 'test3',
            'tag': 'tag1'
        }
    ]

    update_request = {
        'knowledge_base': {
            'rows': test_rows
        }
    }

    update_response = client.put(
        '/api/projects/mindsdb/knowledge_bases/test_kb_valid_metadata',
        json=update_request,
        follow_redirects=True
    )

    assert '200' in update_response.status


def test_document_processing_with_default_metadata(client):
    """Test document processing where system adds default metadata"""
    create_request = {
        'knowledge_base': {
            'name': 'test_kb_default_metadata',
            'model': 'test_embedding_model'
        }
    }

    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)
    assert '201' in create_response.status

    # Only provide content, system should add default metadata
    update_request = {
        'knowledge_base': {
            'rows': [{
                'content': 'Test content with system metadata',
                'source': 'api'  # This will be moved to metadata
            }]
        }
    }

    # Need to patch both controller and preprocessor
    with patch('mindsdb.interfaces.knowledge_base.controller.KnowledgeBaseTable') as mock_kb_table:
        mock_instance = MagicMock()
        mock_kb_table.return_value = mock_instance

        # Mock KB params
        mock_kb = MagicMock()
        mock_kb.params = {}
        mock_instance._kb = mock_kb

        update_response = client.put(
            '/api/projects/mindsdb/knowledge_bases/test_kb_default_metadata',
            json=update_request,
            follow_redirects=True
        )

        assert '200' in update_response.status

        # Verify that insert_rows was called with the correct data
        mock_instance.insert_rows.assert_called_once()
        inserted_rows = mock_instance.insert_rows.call_args[0][0]
        assert len(inserted_rows) == 1
        assert inserted_rows[0]['content'] == 'Test content with system metadata'
        assert inserted_rows[0]['source'] == 'api'


def test_document_loader_with_file_extensions(client):
    """Test document loader handling different file extensions"""
    create_request = {
        'knowledge_base': {
            'name': 'test_kb_file_extensions',
            'model': 'test_embedding_model'
        }
    }

    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)
    assert '201' in create_response.status

    # Create test files
    with patch('mindsdb.interfaces.file.file_controller.FileController') as mock_file_controller:
        # Mock file existence checks
        mock_file_controller.return_value.get_file_path.return_value = MagicMock()

        update_request = {
            'knowledge_base': {
                'files': ['test.md', 'test.html', 'test.pdf']
            }
        }

        with patch('mindsdb.interfaces.knowledge_base.controller.KnowledgeBaseTable') as mock_kb_table:
            mock_table_instance = MagicMock()
            mock_kb_table.return_value = mock_table_instance

            # Mock KB params
            mock_kb = MagicMock()
            mock_kb.params = {}
            mock_table_instance._kb = mock_kb

            update_response = client.put(
                '/api/projects/mindsdb/knowledge_bases/test_kb_file_extensions',
                json=update_request,
                follow_redirects=True
            )

            assert '200' in update_response.status

            # Verify document loader was used
            mock_table_instance.insert_files.assert_called_once_with(['test.md', 'test.html', 'test.pdf'])


def test_document_loader_sql_error_handling(client):
    """Test document loader handling SQL query errors"""
    create_request = {
        'knowledge_base': {
            'name': 'test_kb_sql_errors',
            'model': 'test_embedding_model'
        }
    }

    create_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)
    assert '201' in create_response.status

    update_request = {
        'knowledge_base': {
            'query': 'INVALID SQL QUERY'
        }
    }

    with patch('mindsdb.interfaces.knowledge_base.preprocessing.document_loader.DocumentLoader') as mock_loader_class:
        mock_loader = MagicMock()
        mock_loader_class.return_value = mock_loader
        mock_loader.load_query_result.side_effect = ValueError('Invalid SQL query')

        update_response = client.put(
            '/api/projects/mindsdb/knowledge_bases/test_kb_sql_errors',
            json=update_request,
            follow_redirects=True
        )

        assert '400' in update_response.status


def test_preprocessing_update(client):
    """Test updating preprocessing configuration"""
    initial_request = {
        'knowledge_base': {
            'name': 'test_kb_preprocess_update',
            'model': 'test_embedding_model'
        }
    }

    # First try to delete if exists
    client.delete(
        f'/api/projects/mindsdb/knowledge_bases/{initial_request["knowledge_base"]["name"]}',
        follow_redirects=True
    )

    with patch('mindsdb.api.http.namespaces.knowledge_bases.SessionController') as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        # Setup KB controller mock
        mock_kb_controller = MagicMock()
        mock_session.kb_controller = mock_kb_controller

        # Setup table mock
        mock_table = MagicMock()
        mock_kb_controller.get_table.return_value = mock_table

        # Setup KB object mock
        mock_kb = MagicMock()
        mock_kb.params = {}
        mock_kb.id = 1
        mock_kb.name = initial_request['knowledge_base']['name']
        mock_kb.as_dict.return_value = {
            'id': 1,
            'name': initial_request['knowledge_base']['name'],
            'params': mock_kb.params,
            'created_at': '2024-11-07T12: 13: 46',
            'updated_at': '2024-11-07T12: 13: 46'
        }

        # Setup controller methods
        mock_kb_controller.get.side_effect = [None, mock_kb]
        mock_kb_controller.add.return_value = mock_kb

        create_response = client.post(
            '/api/projects/mindsdb/knowledge_bases',
            json=initial_request,
            follow_redirects=True
        )

        assert '201' in create_response.status

        # Now update with preprocessing config
        update_request = {
            'knowledge_base': {
                'preprocessing': {
                    'type': 'text_chunking',
                    'text_chunking_config': {
                        'chunk_size': 300,
                        'chunk_overlap': 30
                    }
                }
            }
        }

        # Reset get to always return the KB now that it exists
        mock_kb_controller.get.reset_mock()
        mock_kb_controller.get.return_value = mock_kb

        update_response = client.put(
            f'/api/projects/mindsdb/knowledge_bases/{initial_request["knowledge_base"]["name"]}',
            json=update_request,
            follow_redirects=True
        )

        assert '200' in update_response.status

        # Verify preprocessing was updated
        mock_table.configure_preprocessing.assert_called_with(
            update_request['knowledge_base']['preprocessing']
        )
