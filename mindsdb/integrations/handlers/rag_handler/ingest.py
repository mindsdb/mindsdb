import time

import pandas as pd
from langchain.schema import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.vectorstores import VectorStore

from mindsdb.integrations.handlers.rag_handler.settings import (
    PersistedVectorStoreSaver,
    PersistedVectorStoreSaverConfig,
    RAGHandlerParameters,
    VectorStoreFactory,
    df_to_documents,
    get_chroma_settings,
    load_embeddings_model,
    url_to_documents,
)
from mindsdb.utilities.log import get_log

logger = get_log(__name__)


def validate_document(doc) -> bool:
    """Check an individual document."""
    # Example checks
    if not isinstance(doc, Document):
        return False

    if not doc.page_content:
        return False

    return True


def validate_documents(documents) -> bool:
    """Validate document list format."""

    if not isinstance(documents, list):
        return False

    if not documents:
        return False

    # Check fields/format of a document
    return all([validate_document(doc) for doc in documents])


class Ingestor:
    """A class for converting a dataframe and/or url to a vectorstore embedded with a given embeddings model"""

    def __init__(
        self,
        args: RAGHandlerParameters,
        df: pd.DataFrame,
    ):
        self.args = args
        self.df = df
        self.embeddings_model_name = args.embeddings_model_name

        self.vector_store = VectorStoreFactory.get_vectorstore_class(
            args.vector_store_name
        )

    def split_documents(self, chunk_size, chunk_overlap) -> list:
        # Load documents and split in chunks
        logger.info(f"Loading documents from input data")

        documents = []

        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size, chunk_overlap=chunk_overlap
        )

        if self.df is not None:
            # if user provides a dataframe, load documents from dataframe
            documents.extend(
                df_to_documents(
                    df=self.df, page_content_columns=self.args.context_columns
                )
            )

        if self.args.url:
            # if user provides a url, load documents from url
            documents.extend(url_to_documents(self.args.url))

        # split documents into chunks of text
        texts = text_splitter.split_documents(documents)
        logger.info(f"Loaded {len(documents)} documents from input data")
        logger.info(f"Split into {len(texts)} chunks of text (tokens)")

        return texts

    def create_db_from_documents(self, documents, embeddings_model) -> VectorStore:
        """Create DB from documents."""

        if self.args.vector_store_name == "chroma":

            return self.vector_store.from_documents(
                documents=documents,
                embedding=embeddings_model,
                persist_directory=self.args.vector_store_storage_path,
                client_settings=get_chroma_settings(
                    persist_directory=self.args.vector_store_storage_path
                ),
                collection_name=self.args.collection_name,
            )
        else:
            return self.vector_store.from_documents(
                documents=documents,
                embedding=embeddings_model,
                index_name=self.args.collection_name,
            )

    def create_db_from_texts(self, documents, embeddings_model) -> VectorStore:
        """Create DB from text content."""

        texts = [doc.page_content for doc in documents]
        metadata = [doc.metadata for doc in documents]

        return self.vector_store.from_texts(
            texts=texts, embedding=embeddings_model, metadatas=metadata
        )

    def embeddings_to_vectordb(self) -> None:
        """Create vectorstore from documents and store locally."""

        start_time = time.time()

        # Load documents and splits in chunks (if not in evaluation_type mode)
        documents = self.split_documents(
            chunk_size=self.args.chunk_size, chunk_overlap=self.args.chunk_overlap
        )

        # Load embeddings model
        embeddings_model = load_embeddings_model(self.embeddings_model_name)

        logger.info(f"Creating vectorstore from documents")

        if not validate_documents(documents):
            raise ValueError("Invalid documents")

        try:
            db = self.create_db_from_documents(documents, embeddings_model)
        except Exception as e:
            logger.error(
                f"Error loading using 'from_documents' method, trying 'from_text': {e}"
            )
            try:
                db = self.create_db_from_texts(documents, embeddings_model)
                logger.info(f"successfully loaded using 'from_text' method: {e}")

            except Exception as e:
                logger.error(f"Error creating from texts: {e}")
                raise e

        config = PersistedVectorStoreSaverConfig(
            vector_store_name=self.args.vector_store_name,
            vector_store=db,
            persist_directory=self.args.vector_store_storage_path,
            collection_name=self.args.collection_name,
        )

        vector_store_saver = PersistedVectorStoreSaver(config)

        vector_store_saver.save_vector_store(db)

        db = None
        end_time = time.time()
        elapsed_time = end_time - start_time

        logger.info(
            "Fished creating vectorstore from documents. It took: {elapsed_time/60} minutes"
        )

        logger.info("Finished creating vectorstore from documents.")
        logger.info(f"Elapsed time: {round(elapsed_time / 60)} minutes")
