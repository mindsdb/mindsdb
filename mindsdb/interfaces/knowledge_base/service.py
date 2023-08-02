"""
Tasks related to the knowledge base
"""

from functools import lru_cache

import chromadb
import pandas as pd
from langchain.document_loaders import DataFrameLoader
from langchain.embeddings import (
    CohereEmbeddings,
    FakeEmbeddings,
    OpenAIEmbeddings,
    SentenceTransformerEmbeddings,
)
from langchain.embeddings.base import Embeddings
from langchain.retrievers import BM25Retriever, EnsembleRetriever
from langchain.schema.retriever import BaseRetriever
from langchain.vectorstores import Chroma, PGVector
from langchain.vectorstores.base import VectorStore

from mindsdb.interfaces.storage.db import (
    KBEmbeddingModel,
    KBRetrievalStrategy,
    KBVectorDatabase,
    KnowledgeBase,
    engine,
)
from mindsdb.utilities.fs import get_or_create_data_dir
from mindsdb.utilities.log import get_log

LOG = get_log()


class KnowledgeBaseError(Exception):
    """
    Exception raised when there is an error with the knowledge base
    """


def get_chromdb_persistence_path():
    return get_or_create_data_dir() + "/chromadb.db"


def get_embedding_func(embedding_method: KBEmbeddingModel) -> Embeddings:
    """
    Cached function to return an embedding function based on the embedding method
    """
    if embedding_method == KBEmbeddingModel.OPENAI:
        return OpenAIEmbeddings()  # require OPENAI_API_KEY to be set in env
    elif embedding_method == KBEmbeddingModel.SENTENCE_TRANSFORMER:
        return SentenceTransformerEmbeddings()
    elif embedding_method == KBEmbeddingModel.COHERE:
        return CohereEmbeddings()  # require COHERE_API_KEY to be set in env
    elif embedding_method == KBEmbeddingModel.DUMMY:
        return FakeEmbeddings(size=1024)
    else:
        raise NotImplementedError(
            f"Embedding method {embedding_method} is not implemented"
        )


def get_store(knowledge_base: KnowledgeBase) -> VectorStore:
    """
    Cached function to return a langchain vector store object based on the knowledge base
    """
    collection_handle = knowledge_base.collection_handle
    embedding_func = get_embedding_func(knowledge_base.params.embedding_model)
    if knowledge_base.params.vector_database == KBVectorDatabase.CHROMADB:
        # construct a persistent client, with storage backed by a local database
        client = chromadb.PersistentClient(get_chromdb_persistence_path())
        # create a new vector store
        store = Chroma(
            client=client,
            collection_name=collection_handle,
            embedding_function=embedding_func,
        )
    elif knowledge_base.params.vector_database == KBVectorDatabase.POSTGRES:
        # get the postgres connection string from sql_alchemy
        connection_string = engine.url
        # make sure we are connecting to postgres
        if not connection_string.drivername.startswith("postgres"):
            raise ValueError(
                "We are not using postgres as the database, but the knowledge base is configured to use postgres"
            )

        # create a new vector store
        store = PGVector(
            collection_name=collection_handle,
            embedding_function=embedding_func,
            connection_string=connection_string,
        )

    else:
        raise NotImplementedError(
            f"Vector database {knowledge_base.params.vector_database} is not implemented"
        )

    return store


class KnowledgeBaseService:
    def __init__(self, knowledge_base: KnowledgeBase):
        self.knowledge_base = knowledge_base

    def _get_or_create_vector_store(self) -> VectorStore:
        """
        Connect to the underlying collection of vectors for the knowledge base.
        """
        return get_store(self.knowledge_base)

    def _get_or_create_retriever(self) -> BaseRetriever:
        """
        Create a retriever interface based on the knowledge base object.
        This is the main interface for querying the knowledge base.
        """
        if self.knowledge_base.params.retrieval_strategy == KBRetrievalStrategy.BM25:
            raise NotImplementedError("BM25 is not implemented yet")
        elif (
            self.knowledge_base.params.retrieval_strategy == KBRetrievalStrategy.HYBRID
        ):
            raise NotImplementedError("Hybrid is not implemented yet")
        elif (
            self.knowledge_base.params.retrieval_strategy
            == KBRetrievalStrategy.SIMILARITY
        ):
            store = self._get_or_create_vector_store()
            return store.as_retriever()  # default is cosine similarity

        elif self.knowledge_base.params.retrieval_strategy == KBRetrievalStrategy.MMR:
            store = self._get_or_create_vector_store()
            return store.as_retriever(search_type="mmr")
        else:
            raise NotImplementedError(
                f"Retrieval strategy {self.knowledge_base.params.retrieval_strategy} is not implemented"
            )

    def create_index(self, df: pd.DataFrame):
        """
        Create a new index in langchain based on the current knowledge base

        df is the dataframe that contains the documents to be indexed
        Important columns are id, content and metadata
        """
        store = self._get_or_create_vector_store()

        id_col = self.knowledge_base.params.id_field
        content_col = self.knowledge_base.params.content_field
        metadata_cols = self.knowledge_base.params.metadata_fields

        # make sure the id column is present
        if id_col not in df.columns:
            raise KnowledgeBaseError(
                f"ID column {id_col} is not present in the dataframe"
            )

        # if no content column is specified, we convert all non-id columns to a single content column, in the form of
        # "col1: value1, col2: value2, ..."
        if content_col is None:
            df["_mindsdb_kb_content"] = df.apply(
                lambda x: ", ".join(
                    [f"{col}: {x[col]}" for col in df.columns if col != id_col]
                ),
                axis=1,
            )

            content_col = "_mindsdb_kb_content"

        # make sure the content column is present
        if content_col not in df.columns:
            raise KnowledgeBaseError(
                f"Content column {content_col} is not present in the dataframe"
            )

        # if no metadata columns are specified, we use all columns except id and content
        if metadata_cols is None:
            metadata_cols = [col for col in df.columns if col != content_col]

        # create the document objects
        loader = DataFrameLoader(
            data_frame=df,
            page_content_column=content_col,
        )

        # create the index
        store.add_documents(
            loader.load(),
            ids=df[id_col].tolist(),
        )

    def query_index(self, query: str, top_k: int = 10) -> pd.DataFrame:
        """
        Query the index in langchain based on the current knowledge base
        """
        # TODO: add support for metadata filters
        # retriever = self._get_or_create_retriever()

        # search the index and return the scores
        store = self._get_or_create_vector_store()
        docs = store.similarity_search_with_score(
            query,
            k=top_k,
        )

        # convert the results to a dataframe
        df = pd.DataFrame.from_records(
            [
                {
                    "id": doc.metadata.get("id", None),
                    "content": doc.page_content,
                    "metadata": doc.metadata,
                    "score": score,
                }
                for doc, score in docs
            ]
        )

        return df

    def update_index(self, df: pd.DataFrame):
        """
        Update the index in langchain based on the current knowledge base

        This is a bit of tricky, since we want to do upsert as much as possible.
        For non-changed documents, we don't want to recompute the embeddings, to save time.
        For now, we will just delete the index and recreate it.
        """
        self.delete_index()
        self.create_index(df)

    def delete_index(self):
        """
        Delete the index in langchain based on the current knowledge base
        """
        store = self._get_or_create_vector_store()
        store.delete_collection()
        # after collection is deleted, calling the get_or_create_vector_store will create a new collection
