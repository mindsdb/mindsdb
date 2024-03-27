from datetime import timedelta
from typing import List
import time

import pandas as pd
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_core.vectorstores import VectorStore

from mindsdb.integrations.utilities.rag.loaders.vector_store_loader.vector_store_loader import VectorStoreLoader

# gpt-3.5-turbo
_DEFAULT_TPM_LIMIT = 60000
_DEFAULT_RATE_LIMIT_INTERVAL = timedelta(seconds=10)
_INITIAL_TOKEN_USAGE = 0


def df_to_documents(df: pd.DataFrame, content_column_name: str) -> List[Document]:
    """
    Given a dataframe, convert it to a list of documents.

    :param df: pd.DataFrame
    :param content_column_name: str

    :return: List[Document]
    """
    documents = []
    for _, row in df.iterrows():
        metadata = row.to_dict()
        page_content = metadata.pop(content_column_name)
        documents.append(Document(page_content=page_content, metadata=metadata))
    return documents


def documents_to_df(content_column_name: str,
                    documents: List[Document],
                    embeddings_model: Embeddings = None,
                    with_embeddings: bool = False) -> pd.DataFrame:
    """
    Given a list of documents, convert it to a dataframe.

    :param content_column_name: str
    :param documents: List[Document]
    :param embeddings_model: Embeddings
    :param with_embeddings: bool

    :return: pd.DataFrame
    """
    df = pd.DataFrame([doc.metadata for doc in documents])

    df[content_column_name] = [doc.page_content for doc in documents]

    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # Reordering the columns to have the content column first.
    df = df[[content_column_name] + [col for col in df.columns if col != content_column_name]]

    if with_embeddings:
        df["embeddings"] = embeddings_model.embed_documents(df[content_column_name].tolist())

    return df


class VectorStoreOperator:
    """
    Encapsulates the logic for adding documents to a vector store with rate limiting.
    """

    def __init__(self,
                 vector_store: VectorStore,
                 embeddings_model: Embeddings,
                 documents: List[Document] = None,
                 token_per_minute_limit: int = _DEFAULT_TPM_LIMIT,
                 rate_limit_interval: timedelta = _DEFAULT_RATE_LIMIT_INTERVAL,

                 ):

        self.documents = documents
        self.embeddings_model = embeddings_model
        self.token_per_minute_limit = token_per_minute_limit
        self.rate_limit_interval = rate_limit_interval
        self.current_token_usage = _INITIAL_TOKEN_USAGE
        self._vector_store = None

        self.verify_vector_store(vector_store, documents)

    def verify_vector_store(self, vector_store, documents):
        if documents:
            self._add_documents_to_store(documents, vector_store)
        elif isinstance(vector_store, VectorStore):
            # checking is it instance or subclass instance
            self._vector_store = vector_store
        elif issubclass(vector_store, VectorStore):
            # checking is it an uninstantiated subclass of VectorStore i.e. Chroma or PGVector
            raise ValueError("If not documents are provided, an instantiated vector_store must be provided")

    @property
    def vector_store(self):
        return self._vector_store

    @staticmethod
    def _calculate_token_usage(document):
        return len(document.page_content)

    def _rate_limit(self):
        if self.current_token_usage >= self.token_per_minute_limit:
            time.sleep(self.rate_limit_interval.total_seconds())
            self.current_token_usage = _INITIAL_TOKEN_USAGE

    def _update_token_usage(self, document: Document):
        self._rate_limit()
        self.current_token_usage += self._calculate_token_usage(document)

    def _add_document(self, document: Document):
        self._update_token_usage(document)
        self.vector_store.add_documents([document])

    def _add_documents_to_store(self, documents: List[Document], vector_store: VectorStore):
        self._init_vector_store(documents, vector_store)
        self.add_documents(documents)

    def _init_vector_store(self, documents: List[Document], vector_store: VectorStore):
        if len(documents) > 0:
            self._vector_store = vector_store.from_documents(
                documents=[documents[0]], embedding=self.embeddings_model
            )

    def add_documents(self, documents: List[Document]):
        for document in documents:
            self._add_document(document)


def load_vector_store(embeddings_model: Embeddings, config: dict) -> VectorStore:
    """
    Loads the vector store based on the provided config and embeddings model
    :param embeddings_model:
    :param config:
    :return:
    """
    loader = VectorStoreLoader(embeddings_model=embeddings_model, config=config)
    return loader.load()
