from typing import Any

from pydantic import BaseModel

from mindsdb.integrations.utilities.rag.settings import VectorStoreConfig
from mindsdb.integrations.utilities.rag.loaders.vector_store_loader.base_vector_store import VectorStore
from mindsdb.integrations.utilities.rag.loaders.vector_store_loader.MDBVectorStore import MDBVectorStore
from mindsdb.integrations.utilities.rag.loaders.vector_store_loader.pgvector import PGVectorMDB
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class VectorStoreLoader(BaseModel):
    embedding_model: Any  # Embedding model interface
    vector_store: VectorStore = None
    config: VectorStoreConfig = None

    class Config:
        arbitrary_types_allowed = True
        extra = "forbid"
        validate_assignment = True

    def load(self) -> VectorStore:
        """
        Loads the vector store based on the provided config and embeddings model
        :return:
        """
        if (
            self.config.is_sparse is not None
            and self.config.vector_size is not None
            and self.config.kb_table is not None
        ):
            # Only use PGVector store for sparse vectors.
            db_handler = self.config.kb_table.get_vector_db()
            db_args = db_handler.connection_args
            # Assume we are always using PGVector & psycopg2.
            connection_str = f"postgresql+psycopg2://{db_args.get('user')}:{db_args.get('password')}@{db_args.get('host')}:{db_args.get('port')}/{db_args.get('dbname', db_args.get('database'))}"

            return PGVectorMDB(
                connection_string=connection_str,
                collection_name=self.config.kb_table._kb.vector_database_table,
                embedding_function=self.embedding_model,
                is_sparse=self.config.is_sparse,
                vector_size=self.config.vector_size,
            )
        return MDBVectorStore(kb_table=self.config.kb_table)
