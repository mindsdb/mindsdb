import os
import pytest
from tempfile import TemporaryDirectory

from mindsdb.api.http.initialize import initialize_app
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
