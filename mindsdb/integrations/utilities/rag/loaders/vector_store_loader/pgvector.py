from typing import Any, List, Union, Optional, Dict, Tuple

from pgvector.sqlalchemy import SPARSEVEC, Vector
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

from mindsdb.integrations.utilities.rag.loaders.vector_store_loader.base_vector_store import VectorStore
from mindsdb.interfaces.knowledge_base.preprocessing.document_types import SimpleDocument
from mindsdb.utilities import log

logger = log.getLogger(__name__)

# SQLAlchemy declarative base
Base = declarative_base()

_generated_sa_tables = {}


class PGVectorMDB(VectorStore):
    """
    Custom PGVector implementation for mindsdb vector store table structure
    Replaces langchain_community.vectorstores.PGVector
    """

    def __init__(
        self,
        connection_string: str,
        collection_name: str,
        embedding_function: Any = None,
        is_sparse: bool = False,
        vector_size: Optional[int] = None,
        **kwargs
    ):
        """
        Initialize PGVectorMDB
        
        Args:
            connection_string: PostgreSQL connection string
            collection_name: Name of the table/collection
            embedding_function: Embedding function/model
            is_sparse: Whether to use sparse vectors
            vector_size: Size of sparse vectors (required if is_sparse=True)
        """
        self.is_sparse = is_sparse
        if is_sparse and vector_size is None:
            raise ValueError("vector_size is required when is_sparse=True")
        self.vector_size = vector_size
        self.collection_name = collection_name
        self.embedding_function = embedding_function
        
        # Create SQLAlchemy engine
        self._bind = create_engine(connection_string, pool_pre_ping=True)
        
        # Initialize table structure
        self.__post_init__()

    def __post_init__(
        self,
    ) -> None:
        """Initialize SQLAlchemy table structure"""
        collection_name = self.collection_name

        if collection_name not in _generated_sa_tables:

            class EmbeddingStore(Base):
                """Embedding store."""

                __tablename__ = collection_name

                id = sa.Column(sa.Integer, primary_key=True)
                embedding = sa.Column(
                    "embeddings",
                    SPARSEVEC() if self.is_sparse else Vector() if self.vector_size is None else
                    SPARSEVEC(self.vector_size) if self.is_sparse else Vector(self.vector_size)
                )
                document = sa.Column("content", sa.String, nullable=True)
                cmetadata = sa.Column("metadata", JSON, nullable=True)

            _generated_sa_tables[collection_name] = EmbeddingStore

        self.EmbeddingStore = _generated_sa_tables[collection_name]
    
    @property
    def embeddings(self) -> Optional[Any]:
        """Return embedding function if available"""
        return self.embedding_function
    
    def similarity_search(
        self,
        query: str,
        k: int = 4,
        **kwargs: Any,
    ) -> List[SimpleDocument]:
        """Return most similar documents to query"""
        # Get embedding for query
        if self.embedding_function is None:
            raise ValueError("embedding_function is required for similarity_search")
        
        # Embed the query
        query_embedding = self.embedding_function.embed_query(query)
        
        # Query collection
        results = self.__query_collection(query_embedding, k=k, filter=kwargs.get('filter'))
        
        # Convert to SimpleDocument objects
        docs = []
        for result in results:
            embedding_store = result.EmbeddingStore
            page_content = embedding_store.document or ""
            metadata = embedding_store.cmetadata or {}
            docs.append(SimpleDocument(page_content=page_content, metadata=metadata))
        
        return docs
    
    def similarity_search_with_score(
        self,
        query: str,
        k: int = 4,
        **kwargs: Any,
    ) -> List[Tuple[SimpleDocument, float]]:
        """Return most similar documents with scores"""
        # Get embedding for query
        if self.embedding_function is None:
            raise ValueError("embedding_function is required for similarity_search_with_score")
        
        # Embed the query
        query_embedding = self.embedding_function.embed_query(query)
        
        # Query collection
        results = self.__query_collection(query_embedding, k=k, filter=kwargs.get('filter'))
        
        # Convert to SimpleDocument objects with scores
        docs_with_scores = []
        for result in results:
            embedding_store = result.EmbeddingStore
            page_content = embedding_store.document or ""
            metadata = embedding_store.cmetadata or {}
            doc = SimpleDocument(page_content=page_content, metadata=metadata)
            # Distance is already calculated in __query_collection
            score = float(result.distance) if hasattr(result, 'distance') else 0.0
            docs_with_scores.append((doc, score))
        
        return docs_with_scores

    def __query_collection(
            self,
            embedding: Union[List[float], Dict[int, float], str],
            k: int = 4,
            filter: Optional[Dict[str, str]] = None,
    ) -> List[Any]:
        """Query the collection."""
        with Session(self._bind) as session:
            if self.is_sparse:
                # Sparse vectors: expect string in format "{key:value,...}/size" or dictionary
                if isinstance(embedding, dict):
                    from pgvector.utils import SparseVector
                    embedding = SparseVector(embedding, self.vector_size)
                    embedding_str = embedding.to_text()
                elif isinstance(embedding, str):
                    # Use string as is - it should already be in the correct format
                    embedding_str = embedding
                # Use inner product for sparse vectors
                distance_op = "<#>"
                # For inner product, larger values are better matches
                order_direction = "ASC"
            else:
                # Dense vectors: expect string in JSON array format or list of floats
                if isinstance(embedding, list):
                    embedding_str = f"[{','.join(str(x) for x in embedding)}]"
                elif isinstance(embedding, str):
                    embedding_str = embedding
                # Use cosine similarity for dense vectors
                distance_op = "<=>"
                # For cosine similarity, smaller values are better matches
                order_direction = "ASC"

            # Use SQL directly for vector comparison
            query = sa.text(
                f"""
            SELECT t.*, t.embeddings {distance_op} '{embedding_str}' as distance
            FROM {self.collection_name} t
            ORDER BY distance {order_direction}
            LIMIT {k}
            """
            )
            results = session.execute(query).all()

            # Convert results to the expected format
            formatted_results = []
            for rec in results:
                metadata = rec.metadata if bool(rec.metadata) else {0: 0}
                embedding_store = self.EmbeddingStore()
                embedding_store.document = rec.content
                embedding_store.cmetadata = metadata
                result = type(
                    'Result', (), {
                        'EmbeddingStore': embedding_store,
                        'distance': rec.distance
                    }
                )
                formatted_results.append(result)

            return formatted_results

    # Aliases for compatibility
    def _PGVector__query_collection(self, *args, **kwargs):
        return self.__query_collection(*args, **kwargs)

    def _query_collection(self, *args, **kwargs):
        return self.__query_collection(*args, **kwargs)

    def create_collection(self):
        raise RuntimeError("Forbidden")

    def delete_collection(self):
        raise RuntimeError("Forbidden")

    def delete(self, *args, **kwargs):
        raise RuntimeError("Forbidden")

    def add_embeddings(self, *args, **kwargs):
        raise RuntimeError("Forbidden")
