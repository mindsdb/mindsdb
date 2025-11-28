import os
import tempfile
import pandas as pd
from mindsdb.integrations.handlers.duckdb_faiss_handler.duckdb_faiss_handler import DuckDBFaissHandler
from mindsdb.integrations.utilities.sql_utils import KeywordSearchArgs


def test_handler():
    """Test the DuckDB Faiss handler functionality."""

    # Create temporary directory for testing
    with tempfile.TemporaryDirectory() as temp_dir:
        # Mock handler storage
        class MockHandlerStorage:
            def folder_get(self, name):
                return os.path.join(temp_dir, name)

            def folder_sync(self, name):
                pass

        # Initialize handler
        handler = DuckDBFaissHandler(
            name="test_db",
            connection_data={
                "persist_directory": temp_dir,
                "metric": "cosine",
                "backend": "flat",  # Use flat for testing
                "use_gpu": False,
            },
        )

        print("✓ Handler initialized successfully")

        # Create a simple table with vector column
        table_name = "test_table"

        handler.create_table(table_name)
        print("✓ Table created successfully")

        # Test data insertion
        test_data = pd.DataFrame(
            {
                "id": ["doc1", "doc2", "doc3"],
                "content": ["Document 1 content", "Document 2 content", "Document 3 content"],
                "metadata": ['{"category": "news"}', '{"category": "tech"}', '{"category": "news"}'],
                "title": ["News Article 1", "Tech Article 1", "News Article 2"],
                "category": ["news", "tech", "news"],
                "embeddings": [
                    [0.1, 0.2, 0.3] + [0.0] * 381,  # 384-dim vector
                    [0.4, 0.5, 0.6] + [0.0] * 381,
                    [0.7, 0.8, 0.9] + [0.0] * 381,
                ],
            }
        )

        handler.insert(table_name, test_data)
        print("✓ Data inserted successfully")

        # Test vector search
        query_vector = [0.1, 0.2, 0.3] + [0.0] * 381
        from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator

        vector_conditions = [FilterCondition(column="embeddings", op=FilterOperator.EQUAL, value=query_vector)]

        metadata_conditions = [FilterCondition(column="metadata.category", op=FilterOperator.EQUAL, value="news")]

        results = handler.select(
            table_name=table_name, columns=["id", "content", "title", "distance"], conditions=vector_conditions, limit=5
        )

        print(f"✓ Vector search returned {len(results)} results")
        print("Search results:")
        print(results)

        # Test metadata filtering

        metadata_results = handler.select(
            table_name=table_name,
            columns=["id", "content", "metadata"],
            conditions=metadata_conditions + vector_conditions,
        )

        print(f"✓ Metadata filtering returned {len(metadata_results)} results")
        print("Metadata filter results:")
        print(metadata_results)

        # Test keyword search

        hybrid_results = handler.keyword_select(
            table_name=table_name,
            columns=["id", "content", "title", "distance"],
            conditions=metadata_conditions,
            keyword_search_args=KeywordSearchArgs(column="content", query="Document"),
            limit=5,
        )

        print(f"✓ Hybrid search returned {len(hybrid_results)} results")
        print("Hybrid search results:")
        print(hybrid_results)

        # Test table listing
        tables = handler.get_tables()
        print("✓ Tables retrieved successfully")
        print("Tables:", tables.data_frame)

        # Test column listing
        columns = handler.get_columns(table_name)
        print("✓ Columns retrieved successfully")
        print("Columns:", columns.data_frame)

        print("\n🎉 All tests passed successfully!")
