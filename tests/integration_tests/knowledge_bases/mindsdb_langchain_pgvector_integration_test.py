from mindsdb.integrations.utilities.rag.loaders.vector_store_loader.pgvector import PGVectorMDB
from langchain_core.embeddings import Embeddings
from pgvector.utils import SparseVector


class DummyEmbeddings(Embeddings):
    """Dummy embeddings that just return the input vector"""
    def embed_documents(self, texts):
        return [SparseVector({1: 0.5, 5: 0.3, 10: 0.8}, 128)] * len(texts)

    def embed_query(self, text):
        return SparseVector({1: 0.5, 5: 0.3, 10: 0.8}, 128)


def setup_pgvector_database():
    """Setup pgvector database"""
    connection_string = "postgresql://postgres:supersecret@127.0.0.1:5432/test_sparse_vectors"

    # Initialize PGVectorMDB
    vector_db = PGVectorMDB(
        connection_string=connection_string,
        collection_name="sparse_test",
        embedding_function=DummyEmbeddings(),
        is_sparse=True,
        vector_size=128
    )

    return vector_db


def test_vector_queries(vector_db):
    """Test various vector queries"""
    print("\nTesting vector queries...")

    # Create a test vector
    test_vector = SparseVector({1: 0.5, 5: 0.3, 10: 0.8}, 128)

    # Query similar vectors
    results = vector_db._query_collection(
        embedding=test_vector,
        k=2
    )

    print("\nVector similarity search results:")
    for item, distance in results:
        print(f"Content: {item.content}")
        print(f"Metadata: {item.metadata}")
        print(f"Distance: {distance}")
        print("---")


def main():
    # Setup vector database
    print("\nSetting up pgvector database...")
    vector_db = setup_pgvector_database()

    # Run tests
    test_vector_queries(vector_db)


if __name__ == "__main__":
    main()
