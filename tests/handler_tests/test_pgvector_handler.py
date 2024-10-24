import os
import psycopg2
import pytest

from mindsdb.integrations.handlers.pgvector_handler.pgvector_handler import PgVectorHandler


TEST_DB_NAME = os.environ.get('MDB_TEST_PGVECTOR_DATABASE', 'pgvector_handler_test_db')
# Should match table name in data/pgvector/seed.sql
TEST_TABLE_NAME = 'items'
# Should match column names in data/pgvector/seed.sql
COLUMN_NAMES = ['id', 'content', 'embeddings', 'metadata']

HANDLER_KWARGS = {
    'connection_data': {
        'host': os.environ.get('MDB_TEST_PGVECTOR_HOST', '127.0.0.1'),
        'port': os.environ.get('MDB_TEST_PGVECTOR_PORT', '5432'),
        'user': os.environ.get('MDB_TEST_PGVECTOR_USER', 'postgres'),
        'password': os.environ.get('MDB_TEST_PGVECTOR_PASSWORD', 'supersecret'),
        'database': TEST_DB_NAME
    }
}


def init_db():
    '''Seed the test DB with some data'''
    conn_info = HANDLER_KWARGS['connection_data'].copy()
    conn_info['database'] = 'postgres'
    db = psycopg2.connect(**conn_info)
    db.autocommit = True
    cursor = db.cursor()

    try:
        cursor.execute(f'DROP DATABASE IF EXISTS {TEST_DB_NAME};')
        db.commit()

        # Create the test database
        cursor.execute(f'CREATE DATABASE {TEST_DB_NAME};')
        db.commit()

        # Reconnect to the new database
        conn_info['database'] = TEST_DB_NAME
        db = psycopg2.connect(**conn_info)
        db.autocommit = True
        cursor = db.cursor()

        # Seed the database with data
        curr_dir = os.path.dirname(os.path.realpath(__file__))
        seed_sql_path = os.path.join(curr_dir, 'data', 'pgvector', 'seed.sql')
        with open(seed_sql_path, 'r') as sql_seed_file:
            cursor.execute(sql_seed_file.read())
        db.commit()

    finally:
        # Close the cursor and the connection
        cursor.close()
        db.close()


@pytest.fixture(scope='module')
def handler():
    init_db()
    handler = PgVectorHandler('test_handler', **HANDLER_KWARGS)
    yield handler


@pytest.mark.skipif(os.environ.get('MDB_TEST_PGVECTOR_HOST') is None, reason='MDB_TEST_PGVECTOR_HOST environment variable not set')
class TestPgvectorConnection:
    def test_connect(self, handler):
        handler.connect()
        assert handler.is_connected, 'connection error'

    def test_check_connection(self, handler):
        res = handler.check_connection()
        assert res.success, res.error_message


@pytest.mark.skipif(os.environ.get('MDB_TEST_PGVECTOR_HOST') is None, reason='MDB_TEST_PGVECTOR_HOST environment variable not set')
class TestPgvectorQuery:
    def test_select(self, handler):
        result = handler.select(TEST_TABLE_NAME)
        assert not result.empty
        for col in COLUMN_NAMES:
            assert col in result.columns

    def test_hybrid_search_with_keywords(self, handler):
        result = handler.hybrid_search(
            TEST_TABLE_NAME,
            # Embeddings (semantic) search.
            [7.0, 8.0, 9.0],
            # Keyword search.
            query='cat rat'
        )
        # Top result is an exact embeddings match.
        assert result.iloc[0]['embeddings'].tolist() == [7.0, 8.0, 9.0]
        # Top result should include both keywords.
        assert 'cat' in result.iloc[0]['content']
        assert 'rat' in result.iloc[0]['content']

    def test_hybrid_search_with_metadata(self, handler):
        result = handler.hybrid_search(
            TEST_TABLE_NAME,
            # Embeddings (semantic) search.
            [4.0, 5.0, 6.0],
            # Metadata filters.
            metadata={'location': 'Wonderland', 'author': 'Taishan'}
        )
        # Only two items match metadata filters.
        assert len(result.index) == 2
        # Top result is an exact embeddings match.
        assert result.iloc[0]['embeddings'].tolist() == [4.0, 5.0, 6.0]

    def test_hybrid_search_with_keywords_and_metadata(self, handler):
        result = handler.hybrid_search(
            TEST_TABLE_NAME,
            # Embeddings (semantic) search.
            [4.0, 5.0, 6.0],
            # Keyword search.
            query='fat cat',
            # Metadata filters.
            metadata={'location': 'Wonderland', 'author': 'Taishan'}
        )
        # Only two items match metadata filters.
        assert len(result.index) == 2
        # Top result is actually a keyword match because embeddings are close.
        assert result.iloc[0]['embeddings'].tolist() == [1.0, 2.0, 3.0]

    def test_hybrid_search_no_query_or_metadata(self, handler):
        with pytest.raises(ValueError):
            _ = handler.hybrid_search(
                TEST_TABLE_NAME,
                # Embeddings (semantic) search.
                [4.0, 5.0, 6.0],
            )
