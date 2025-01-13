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
        embedding: List[Union[float]],
        k: int = 4,
        filter: Optional[Dict[str, str]] = None,
    ) -> List[Any]:
        """Query the collection."""
        with Session(self._bind) as session:
            # Convert embedding to text format
            if isinstance(embedding, dict):
                from pgvector.utils import SparseVector
                embedding = SparseVector(embedding, self.vector_size)
            embedding_str = embedding.to_text()

            # Use SQL directly for vector comparison with inner product
            query = sa.text(
                f"""
            SELECT t.*, t.embeddings <#> '{embedding_str}' as distance
            FROM {self.collection_name} t
            ORDER BY distance ASC
            LIMIT {k}
            """)
            results = session.execute(query).all()

        for rec in results:
            if not bool(rec.metadata):
                rec = rec._replace(metadata={0: 0})

        return [(rec, rec.distance) for rec in results]

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
