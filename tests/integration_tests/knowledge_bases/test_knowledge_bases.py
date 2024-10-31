from http import HTTPStatus
from tempfile import TemporaryDirectory
from time import perf_counter, sleep
from uuid import uuid4

import os
import psycopg2
import pytest

from mindsdb.api.http.initialize import initialize_app
from mindsdb.migrations import migrate
from mindsdb.interfaces.storage import db
from mindsdb.utilities.config import Config

# Should match table name in data/seed.sql
TEST_TABLE_NAME = 'items'
# Should match column names in data/seed.sql
COLUMN_NAMES = ['id', 'content', 'embeddings', 'metadata']

CONNECTION_KWARGS = {
    'connection_data': {
        'host': os.environ.get('MDB_TEST_PGVECTOR_HOST', '127.0.0.1'),
        'port': os.environ.get('MDB_TEST_PGVECTOR_PORT', '5432'),
        'user': os.environ.get('MDB_TEST_PGVECTOR_USER', 'postgres'),
        'password': os.environ.get('MDB_TEST_PGVECTOR_PASSWORD', 'supersecret'),
        'database': None  # Different for each test.
    }
}

MODEL_WAIT_DURATION_SECONDS = 5
MODEL_WAIT_SLEEP_INTERVAL_SECONDS = 0.2


@pytest.fixture(scope="session", autouse=True)
def app():
    old_minds_db_con = ''
    if 'MINDSDB_DB_CON' in os.environ:
        old_minds_db_con = os.environ['MINDSDB_DB_CON']
    with TemporaryDirectory(prefix='knowledge_bases_integration_test_') as temp_dir:
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

        # Create model to use in all tests. Use OpenAI for embeddings.
        create_query = '''
        CREATE MODEL mindsdb.test_embedding_model
        PREDICT embeddings
        USING
            engine='langchain_embedding',
            class = 'OpenAIEmbeddings',
            input_columns = ['content'];
        '''
        train_data = {
            'query': create_query
        }
        response = test_client.post('/api/projects/mindsdb/models', json=train_data, follow_redirects=True)
        assert '201' in response.status

        # Wait for model to complete.
        model_complete = False
        model_start_time = perf_counter()
        while not model_complete:
            if (perf_counter() - model_start_time) > MODEL_WAIT_DURATION_SECONDS:
                pytest.fail('Model test_embedding_model did not finish training in time')
            response = test_client.get('/api/projects/mindsdb/models/test_embedding_model')
            model_status = response.get_json().get('status', 'error')
            if model_status == 'complete':
                model_complete = True
                continue
            if model_status == 'error':
                pytest.fail('Model test_embedding_model encountered an error while training')
            sleep(MODEL_WAIT_SLEEP_INTERVAL_SECONDS)
        yield app
    os.environ['MINDSDB_DB_CON'] = old_minds_db_con


@pytest.fixture()
def client(app):
    return app.test_client()


def init_db():
    '''Initialize a new DB for every test.'''
    conn_info = CONNECTION_KWARGS['connection_data'].copy()
    conn_info['database'] = 'postgres'
    db = psycopg2.connect(**conn_info)
    db.autocommit = True
    cursor = db.cursor()

    try:
        new_db_name = f'test_pgvector_{uuid4().hex}'
        # Create the test database if it does not exist.
        cursor.execute(f'DROP DATABASE IF EXISTS {new_db_name}')
        db.commit()
        cursor.execute(f'CREATE DATABASE {new_db_name};')
        db.commit()

        # Reconnect to the new database
        conn_info['database'] = new_db_name
        db = psycopg2.connect(**conn_info)
        db.autocommit = True
        cursor = db.cursor()

        # Seed the database with data
        curr_dir = os.path.dirname(os.path.realpath(__file__))
        seed_sql_path = os.path.join(curr_dir, 'data', 'seed.sql')
        with open(seed_sql_path, 'r') as sql_seed_file:
            cursor.execute(sql_seed_file.read())
        db.commit()

    finally:
        # Close the cursor and the connection
        cursor.close()
        db.close()

    return new_db_name


@pytest.fixture(autouse=True)
def pgvector_database_name(client):
    # Initialize a fresh DB for each test.
    new_db_name = init_db()
    # Connect new DB to MindsDB.
    conn_info = CONNECTION_KWARGS['connection_data'].copy()
    conn_info['database'] = new_db_name
    example_db_data = {
        'database': {
            'name': new_db_name,
            'engine': 'pgvector',
            'parameters': conn_info
        }
    }
    response = client.post('/api/databases', json=example_db_data, follow_redirects=True)
    assert '201' in response.status
    return new_db_name


@pytest.mark.skipif(os.environ.get('MDB_TEST_PGVECTOR_HOST') is None, reason='MDB_TEST_PGVECTOR_HOST environment variable not set')
@pytest.mark.skipif(os.environ.get('OPENAI_API_KEY') is None, reason='OPENAI_API_KEY environment variable not set')
class TestKnowledgeBaseCompletion:
    def test_chat_completion(self, client, pgvector_database_name):
        test_kb_name = 'test_chat_completion_kb'
        create_request = {
            'knowledge_base': {
                'name': test_kb_name,
                'model': 'test_embedding_model',
                'storage': {
                    'table': TEST_TABLE_NAME,
                    'database': pgvector_database_name
                }
            }
        }
        create_kb_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)
        assert create_kb_response.status_code == HTTPStatus.CREATED

        # Insert documents to help answer the question.
        rows_to_insert = [
            {'content': 'The capital of Tyler Fantasy RAG Land is MindsDB'}
        ]
        update_request = {
            'knowledge_base': {
                'rows': rows_to_insert
            }
        }
        update_kb_response = client.put(f'/api/projects/mindsdb/knowledge_bases/{test_kb_name}',
                                        json=update_request, follow_redirects=True)
        assert update_kb_response.status_code == HTTPStatus.OK

        completion_request = {
            'query': 'What is the capital of Tyler Fantasy RAG Land?',
            'llm_model': 'gpt-4o'
        }
        response = client.post(f'/api/projects/mindsdb/knowledge_bases/{test_kb_name}/completions',
                               json=completion_request, follow_redirects=True)
        assert response.status_code == HTTPStatus.OK
        response_data = response.get_json()
        assert 'message' in response_data
        assert 'content' in response_data['message']
        assert 'context' in response_data['message']
        assert response_data['message']['role'] == 'assistant'
        # Should get the right answer.
        assert 'mindsdb' in response_data['message']['content'].lower()

    def test_context_completion_with_keywords(self, client, pgvector_database_name):
        test_kb_name = 'test_context_completion_with_keywords'
        create_request = {
            'knowledge_base': {
                'name': test_kb_name,
                'model': 'test_embedding_model',
                'storage': {
                    'table': TEST_TABLE_NAME,
                    'database': pgvector_database_name
                }
            }
        }
        create_kb_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)
        assert create_kb_response.status_code == HTTPStatus.CREATED

        # Insert documents for context.
        rows_to_insert = [
            {'content': 'The capital of Tyler Fantasy RAG Land is MindsDB'},
            {'content': 'The population of Tyler Fantasy RAG Land is 6'}
        ]
        update_request = {
            'knowledge_base': {
                'rows': rows_to_insert
            }
        }
        update_kb_response = client.put(f'/api/projects/mindsdb/knowledge_bases/{test_kb_name}',
                                        json=update_request, follow_redirects=True)
        assert update_kb_response.status_code == HTTPStatus.OK

        completion_request = {
            'query': 'Population of rag land',
            'keywords': 'population rag land',
            'type': 'context'
        }
        response = client.post(f'/api/projects/mindsdb/knowledge_bases/{test_kb_name}/completions',
                               json=completion_request, follow_redirects=True)
        assert response.status_code == HTTPStatus.OK
        response_data = response.get_json()
        # Should have the most relevant document first.
        assert 'documents' in response_data
        assert len(response_data['documents']) == 2
        assert 'population' in response_data['documents'][0]['content']

    def test_context_completion_with_metadata(self, client, pgvector_database_name):
        test_kb_name = 'test_context_completion_with_metadata'
        create_request = {
            'knowledge_base': {
                'name': test_kb_name,
                'model': 'test_embedding_model',
                'storage': {
                    'table': TEST_TABLE_NAME,
                    'database': pgvector_database_name
                }
            }
        }
        create_kb_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)
        assert create_kb_response.status_code == HTTPStatus.CREATED

        # Insert documents for context.
        rows_to_insert = [
            {'content': 'The capital of Tyler Fantasy RAG Land is MindsDB', 'author': 'Danya'},
            {'content': 'The population of Tyler Fantasy RAG Land is 6', 'author': 'Tyler'},
            {'content': 'Totally unrelated', 'author': 'Tyler'}

        ]
        update_request = {
            'knowledge_base': {
                'rows': rows_to_insert
            }
        }
        update_kb_response = client.put(f'/api/projects/mindsdb/knowledge_bases/{test_kb_name}',
                                        json=update_request, follow_redirects=True)
        assert update_kb_response.status_code == HTTPStatus.OK

        completion_request = {
            'query': 'Population of rag land',
            'metadata': {
                'author': 'Tyler'
            },
            'type': 'context'
        }
        response = client.post(f'/api/projects/mindsdb/knowledge_bases/{test_kb_name}/completions',
                               json=completion_request, follow_redirects=True)
        assert response.status_code == HTTPStatus.OK
        response_data = response.get_json()
        assert 'documents' in response_data
        # Only 2 have matching metadata.
        assert len(response_data['documents']) == 2
        # Should have the most relevant document first.
        assert 'population' in response_data['documents'][0]['content']

    def test_context_completion_with_keywords_and_metadata(self, client, pgvector_database_name):
        test_kb_name = 'test_context_completion_with_keywords_and_metadata'
        create_request = {
            'knowledge_base': {
                'name': test_kb_name,
                'model': 'test_embedding_model',
                'storage': {
                    'table': TEST_TABLE_NAME,
                    'database': pgvector_database_name
                }
            }
        }
        create_kb_response = client.post('/api/projects/mindsdb/knowledge_bases', json=create_request, follow_redirects=True)
        assert create_kb_response.status_code == HTTPStatus.CREATED

        # Insert documents for context.
        rows_to_insert = [
            {'content': 'The capital of Tyler Fantasy RAG Land is MindsDB', 'author': 'Danya'},
            {'content': 'The population of Tyler Fantasy RAG Land is 6', 'author': 'Tyler'},
            {'content': 'Totally unrelated', 'author': 'Tyler'}

        ]
        update_request = {
            'knowledge_base': {
                'rows': rows_to_insert
            }
        }
        update_kb_response = client.put(f'/api/projects/mindsdb/knowledge_bases/{test_kb_name}',
                                        json=update_request, follow_redirects=True)
        assert update_kb_response.status_code == HTTPStatus.OK

        completion_request = {
            'query': 'Population of rag land',
            'metadata': {
                'author': 'Tyler'
            },
            'keywords': 'rag land population',
            'type': 'context'
        }
        response = client.post(f'/api/projects/mindsdb/knowledge_bases/{test_kb_name}/completions',
                               json=completion_request, follow_redirects=True)
        assert response.status_code == HTTPStatus.OK
        response_data = response.get_json()
        assert 'documents' in response_data
        # Only 2 have matching metadata.
        assert len(response_data['documents']) == 2
        # Should have the most relevant document first.
        assert 'population' in response_data['documents'][0]['content']
