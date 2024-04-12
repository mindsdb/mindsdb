import ast
import uuid

from langchain_core.embeddings import Embeddings
from langchain_community.vectorstores import Chroma, PGVector
from langchain_core.vectorstores import VectorStore
from pydantic import BaseModel

from mindsdb.integrations.libs.vectordatabase_handler import TableField
from mindsdb.integrations.utilities.rag.settings import VectorStoreType, VectorStoreConfig
from mindsdb.utilities import log

import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.exc import DisconnectionError

logger = log.getLogger(__name__)


class VectorStoreLoader(BaseModel):
    embeddings_model: Embeddings
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
        self.vector_store = VectorStoreFactory.create(self.embeddings_model, self.config)
        return self.vector_store


class VectorStoreFactory:
    @staticmethod
    def create(embeddings_model: Embeddings, config: VectorStoreConfig):

        if config.vector_store_type == VectorStoreType.CHROMA:
            return VectorStoreFactory._load_chromadb_store(embeddings_model, config)
        elif config.vector_store_type == VectorStoreType.PGVECTOR:
            return VectorStoreFactory._load_pgvector_store(embeddings_model, config)
        else:
            raise ValueError(f"Invalid vector store type, must be one either {VectorStoreType.__members__.keys()}")

    @staticmethod
    def _load_chromadb_store(embeddings_model: Embeddings, settings) -> Chroma:
        return Chroma(
            persist_directory=settings.persist_directory,
            collection_name=settings.collection_name,
            embedding_function=embeddings_model,
        )

    @staticmethod
    def _load_pgvector_store(embeddings_model: Embeddings, settings) -> PGVector:
        # create an empty store if collection_name does not exist otherwise load the existing collection
        store = PGVector(
            connection_string=settings.connection_string,
            collection_name=settings.collection_name,
            embedding_function=embeddings_model
        )
        return VectorStoreFactory._load_data_into_langchain_pgvector(settings, store)

    @staticmethod
    def _load_data_into_langchain_pgvector(settings, vectorstore: PGVector) -> PGVector:
        """
        Fetches data from the existing pgvector table and loads it into the langchain pgvector vector store
        :param settings:
        :param vectorstore:
        :return:
        """
        df = VectorStoreFactory._fetch_data_from_db(settings)

        df[TableField.EMBEDDINGS] = df[TableField.EMBEDDINGS].apply(ast.literal_eval)
        df[TableField.METADATA] = df[TableField.METADATA].apply(ast.literal_eval)

        metadata = df[TableField.METADATA].tolist()
        embeddings = df[TableField.EMBEDDINGS].tolist()
        texts = df[TableField.CONTENT].tolist()
        ids = [str(uuid.uuid1()) for _ in range(len(df))] \
            if TableField.ID not in df.columns else df[TableField.ID].tolist()

        vectorstore.add_embeddings(
            texts=texts,
            embeddings=embeddings,
            metadatas=metadata,
            ids=ids
        )
        return vectorstore

    @staticmethod
    def _fetch_data_from_db(settings: VectorStoreConfig) -> pd.DataFrame:
        """
        Fetches data from the database using the provided connection_string in the settings
        :param settings:
        :return:
        """
        try:
            engine = create_engine(settings.connection_string)
            db = scoped_session(sessionmaker(bind=engine))

            df = pd.read_sql(f"SELECT * FROM {settings.collection_name}", engine)

            return df
        except DisconnectionError as e:
            logger.error("Unable to connect to the database. Please check your connection string and try again.")
            raise e
        except Exception as e:
            logger.error(f"An error occurred while fetching data from the database: {e}")
            raise e
        finally:
            db.close()
