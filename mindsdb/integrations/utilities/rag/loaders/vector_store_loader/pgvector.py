from typing import Any, List, Union, Optional, Dict

from langchain_community.vectorstores import PGVector
from langchain_community.vectorstores.pgvector import Base

from pgvector.sqlalchemy import SPARSEVEC, Vector
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSON

from sqlalchemy.orm import Session


_generated_sa_tables = {}


class PGVectorMDB(PGVector):
    """
    langchain_community.vectorstores.PGVector adapted for mindsdb vector store table structure
    """

    def __init__(self, *args, is_sparse: bool = False, vector_size: Optional[int] = None, **kwargs):
        # todo get is_sparse and vector_size from kb vector table
        self.is_sparse = is_sparse
        if is_sparse and vector_size is None:
            raise ValueError("vector_size is required when is_sparse=True")
        self.vector_size = vector_size
        super().__init__(*args, **kwargs)

    def __post_init__(
        self,
    ) -> None:

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

    # aliases for different langchain versions
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
