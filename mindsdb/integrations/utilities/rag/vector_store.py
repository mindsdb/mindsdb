import time
from datetime import timedelta
from typing import List

from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_core.vectorstores import VectorStore

from mindsdb.integrations.utilities.rag.loaders.vector_store_loader.vector_store_loader import VectorStoreLoader
from mindsdb.integrations.utilities.rag.settings import VectorStoreConfig, SearchKwargs

# gpt-3.5-turbo
_DEFAULT_TPM_LIMIT = 60000
_DEFAULT_RATE_LIMIT_INTERVAL = timedelta(seconds=10)
_INITIAL_TOKEN_USAGE = 0


class VectorStoreOperator:
    """
    Encapsulates the logic for adding documents to a vector store with rate limiting.
    """

    def __init__(self,
                 vector_store: VectorStore,
                 embedding_model: Embeddings,
                 documents: List[Document] = None,
                 vector_store_config: VectorStoreConfig = None,
                 token_per_minute_limit: int = _DEFAULT_TPM_LIMIT,
                 rate_limit_interval: timedelta = _DEFAULT_RATE_LIMIT_INTERVAL,
                 search_kwargs: SearchKwargs = None

                 ):

        self.documents = documents
        self.embedding_model = embedding_model
        self.token_per_minute_limit = token_per_minute_limit
        self.rate_limit_interval = rate_limit_interval
        self.current_token_usage = _INITIAL_TOKEN_USAGE
        self._vector_store = None
        self.vector_store_config = vector_store_config
        self.search_kwargs = search_kwargs or SearchKwargs()

        self.verify_vector_store(vector_store, documents)

    def verify_vector_store(self, vector_store, documents):
        if documents:
            self._add_documents_to_store(documents, vector_store)
        elif isinstance(vector_store, VectorStore):
            # checking is it instance or subclass instance
            self._vector_store = vector_store
        elif issubclass(vector_store, VectorStore):
            # if it is subclass instance, then create instance of it using vector_store_config
            self._vector_store = load_vector_store(self.embedding_model, self.vector_store_config)

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
                documents=[documents[0]], embedding=self.embedding_model
            )

    def add_documents(self, documents: List[Document]):
        for document in documents:
            self._add_document(document)


def load_vector_store(embedding_model: Embeddings, config: VectorStoreConfig) -> VectorStore:
    """
    Loads the vector store based on the provided config and embeddings model
    :param embedding_model:
    :param config:
    :return:
    """
    loader = VectorStoreLoader(embedding_model=embedding_model, config=config)
    return loader.load()
