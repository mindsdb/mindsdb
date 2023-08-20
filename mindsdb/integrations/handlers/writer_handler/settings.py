from dataclasses import dataclass
from enum import Enum, unique
from functools import lru_cache
from typing import List, Union

import pandas as pd
import torch
from chromadb import Settings
from langchain.callbacks.streaming_stdout import StreamingStdOutCallbackHandler
from langchain.docstore.document import Document
from langchain.document_loaders import DataFrameLoader
from langchain.embeddings.huggingface import HuggingFaceEmbeddings
from langchain.vectorstores import FAISS, Chroma, VectorStore
from llama_index import ServiceContext, SimpleDirectoryReader, VectorStoreIndex
from llama_index.embeddings import LangchainEmbedding
from llama_index.storage.storage_context import StorageContext
from llama_index.vector_stores import ChromaVectorStore
from pydantic import BaseModel, Extra, validator

# todo llama index from chroma vector store


DEFAULT_EMBEDDINGS_MODEL = "sentence-transformers/all-mpnet-base-v2"
USER_DEFINED_WRITER_LLM_PARAMS = (
    "model_id",
    "max_tokens",
    "temperature",
    "top_p",
    "stop",
    "best_of",
    "verbose",
    "writer_org_id",
    "writer_api_key",
    "base_url",
)

SUPPORTED_VECTOR_STORES = ("chroma", "faiss")


def is_valid_store(name):
    return name in SUPPORTED_VECTOR_STORES


class VectorStoreFactory:
    @staticmethod
    def get_vectorstore(name):

        if not isinstance(name, str):
            raise TypeError("name must be a string")

        if not is_valid_store(name):
            raise ValueError(f"Invalid vector store {name}")

        if name == "faiss":
            return FAISS

        if name == "chroma":
            return Chroma


@dataclass
class VectorStoreConfig:
    embeddings_model_name: str
    persist_directory: str
    collection_or_index_name: str


def get_chroma_settings(persist_directory: str = "chromadb"):
    return Settings(
        chroma_db_impl="duckdb+parquet",
        persist_directory=persist_directory,
        anonymized_telemetry=False,
    )


class VectorStoreLoader:
    def __init__(self, config: VectorStoreConfig):
        self.config = config

    def load_vector_store_client(
        self,
        embeddings_model_name: str,
        persist_directory: str,
        collection_or_index_name: str,
        vector_store: str,
    ):

        embeddings_model = load_embeddings_model(embeddings_model_name)

        if vector_store == "chroma":

            return Chroma(
                collection_name=collection_or_index_name,
                persist_directory=persist_directory,
                embedding_function=embeddings_model,
                client_settings=get_chroma_settings(
                    persist_directory=persist_directory
                ),
            )

        elif vector_store == "faiss":

            return FAISS.load_local(
                folder_path=persist_directory,
                embeddings=embeddings_model,
                index_name=collection_or_index_name,
            )

        else:
            raise NotImplementedError(f"{vector_store} client is not yet supported")

    def load_vector_store(self, store_name: str) -> VectorStore:
        method_name = f"load_{store_name}"
        return getattr(self, method_name)()

    def load_chroma(self) -> Chroma:
        return self.load_vector_store_client(
            vector_store="chroma", **self.config.__dict__
        )

    def load_faiss(self) -> FAISS:
        return self.load_vector_store_client(
            vector_store="faiss", **self.config.__dict__
        )


class WriterLLMParameters(BaseModel):
    """Model parameters for the Writer LLM API interface"""

    writer_api_key: str = None
    writer_org_id: str = None
    base_url: str = None
    model_id: str = "palmyra-x"
    callbacks: List[StreamingStdOutCallbackHandler] = [StreamingStdOutCallbackHandler()]
    max_tokens: int = 1024
    temperature: float = 0.0
    top_p: float = 1
    stop: List[str] = []
    best_of: int = 5
    verbose: bool = False

    class Config:
        extra = Extra.forbid
        arbitrary_types_allowed = True


class WriterHandlerParameters(BaseModel):
    """Model parameters for create model"""

    run_embeddings: bool = True
    embeddings_model_name: str = DEFAULT_EMBEDDINGS_MODEL
    prompt_template: str
    llm_params: WriterLLMParameters = WriterLLMParameters()
    context_columns: Union[List[str], str] = None
    vector_store_name: str = "chroma"
    vector_store: VectorStore = VectorStoreFactory.get_vectorstore(vector_store_name)
    collection_or_index_name: str = "langchain"
    vector_store_folder_name: str = "chromadb"
    vector_store_storage_path: str = None

    class Config:
        extra = Extra.forbid
        arbitrary_types_allowed = True
        use_enum_values = True

    @validator("vector_store_name")
    def name_must_be_lower(cls, v):
        return v.lower()


class DfLoader(DataFrameLoader):

    """
    override the load method of langchain.document_loaders.DataFrameLoaders to ignore rows with 'None' values
    """

    def __init__(self, data_frame: pd.DataFrame, page_content_column: str):
        super().__init__(data_frame=data_frame, page_content_column=page_content_column)
        self._data_frame = data_frame
        self._page_content_column = page_content_column

    def load(self) -> List[Document]:
        """Loads the dataframe as a list of documents"""
        documents = []
        for n_row, frame in self._data_frame[self._page_content_column].items():
            if pd.notnull(frame):
                # ignore rows with None values
                column_name = self._page_content_column

                document_contents = frame

                documents.append(
                    Document(
                        page_content=document_contents,
                        metadata={
                            "source": "dataframe",
                            "row": n_row,
                            "column": column_name,
                        },
                    )
                )
        return documents


def df_to_documents(
    df: pd.DataFrame, page_content_columns: Union[List[str], str]
) -> List[Document]:
    """Converts a given dataframe to a list of documents"""
    documents = []

    if isinstance(page_content_columns, str):
        page_content_columns = [page_content_columns]

    for _, page_content_column in enumerate(page_content_columns):
        if page_content_column not in df.columns.tolist():
            raise ValueError(
                f"page_content_column {page_content_column} not in dataframe columns"
            )

        loader = DfLoader(data_frame=df, page_content_column=page_content_column)
        documents.extend(loader.load())

    return documents


@lru_cache()
def load_embeddings_model(embeddings_model_name):
    try:
        model_kwargs = {"device": "gpu" if torch.cuda.is_available() else "cpu"}
        embedding_model = HuggingFaceEmbeddings(
            model_name=embeddings_model_name, model_kwargs=model_kwargs
        )
    except ValueError:
        raise ValueError(
            f"The {embeddings_model_name}  is not supported, please select a valid option from Hugging Face Hub!"
        )
    return embedding_model
