from mindsdb.integrations.utilities.rag.loaders.vector_store_loader.pgvector import PGVectorMDB
from mindsdb.integrations.handlers.langchain_embedding_handler.fastapi_embeddings import FastAPIEmbeddings


def setup_pgvector_database():
    """Setup pgvector database"""
    # Using port 15432 to avoid conflicts with local PostgreSQL
    connection_string = "postgresql://gateway:gateway@localhost:15432/gateway"

    print(f"Connecting to: {connection_string}")

    # Initialize FastAPI embeddings
    embeddings = FastAPIEmbeddings(
        api_base="http://localhost:8043/v1/embeddings",
        model="sparse_model"
    )

    # Initialize PGVectorMDB
    vector_db = PGVectorMDB(
        connection_string=connection_string,
        collection_name="test_dev_doc_vectors",
        embedding_function=embeddings,
        is_sparse=True,  # Using sparse vectors
        vector_size=30522  # Size for sparse vectors
    )

    return vector_db


def test_vector_queries(vector_db):
    """Test various vector queries"""
    print("\nTesting vector queries...")

    # Test text to be embedded
    test_text = "For the Bsecondaryl containment"

    # Get embeddings for the test text
    embedding = vector_db.embedding_function.embed_query(test_text)

    # Query similar vectors
    results = vector_db._query_collection(
        embedding=embedding,
        k=5
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
