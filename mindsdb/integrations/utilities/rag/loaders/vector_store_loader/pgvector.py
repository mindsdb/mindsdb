from typing import Any, List, Optional, Dict

from langchain_community.vectorstores import PGVector
from langchain_community.vectorstores.pgvector import Base

from pgvector.sqlalchemy import Vector
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSON

from sqlalchemy.orm import Session


_generated_sa_tables = {}


class PGVectorMDB(PGVector):
    """
       langchain_community.vectorstores.PGVector adapted for mindsdb vector store table structure
    """

    def __post_init__(
        self,
    ) -> None:

        collection_name = self.collection_name

        if collection_name not in _generated_sa_tables:

            class EmbeddingStore(Base):
                """Embedding store."""

                __tablename__ = collection_name

                id = sa.Column(sa.Integer, primary_key=True)
                embedding: Vector = sa.Column('embeddings', Vector())
                document = sa.Column('content', sa.String, nullable=True)
                cmetadata = sa.Column('metadata', JSON, nullable=True)

            _generated_sa_tables[collection_name] = EmbeddingStore

        self.EmbeddingStore = _generated_sa_tables[collection_name]

    def __query_collection(
        self,
        embedding: List[float],
        k: int = 4,
        filter: Optional[Dict[str, str]] = None,
    ) -> List[Any]:
        """Query the collection."""
        with Session(self._bind) as session:

            results: List[Any] = (
                session.query(
                    self.EmbeddingStore,
                    self.distance_strategy(embedding).label("distance"),
                )
                .order_by(sa.asc("distance"))
                .limit(k)
                .all()
            )
        for rec, _ in results:
            if not bool(rec.cmetadata):
                rec.cmetadata = {0: 0}

        return results

    # aliases for different langchain versions
    def _PGVector__query_collection(self, *args, **kwargs):
        return self.__query_collection(*args, **kwargs)

    def _query_collection(self, *args, **kwargs):
        return self.__query_collection(*args, **kwargs)

    def create_collection(self):
        raise RuntimeError('Forbidden')

    def delete_collection(self):
        raise RuntimeError('Forbidden')

    def delete(self, *args, **kwargs):
        raise RuntimeError('Forbidden')

    def add_embeddings(self, *args, **kwargs):
        raise RuntimeError('Forbidden')
